// Copyright (c) 2016-2017 Tigera, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ipam

import (
	"context"
	"errors"
	"fmt"

	log "github.com/sirupsen/logrus"

	bapi "github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	cerrors "github.com/projectcalico/libcalico-go/lib/errors"
	"github.com/projectcalico/libcalico-go/lib/names"
	"github.com/projectcalico/libcalico-go/lib/net"
)

const (
	// Number of retries when we have an error writing data
	// to etcd.
	ipamEtcdRetries   = 100
	ipamKeyErrRetries = 3
)

// NewIPAMClient returns a new ipamClient, which implements Interface.
// Consumers of the Calico API should not create this directly, but should
// access IPAM through the main client IPAM accessor (e.g. clientv3.IPAM())
func NewIPAMClient(client bapi.Client, pools PoolAccessorInterface) Interface {
	return &ipamClient{
		client: client,
		pools:  pools,
		blockReaderWriter: blockReaderWriter{
			client: client,
			pools:  pools,
		},
	}
}

// ipamClient implements Interface
type ipamClient struct {
	client            bapi.Client
	pools             PoolAccessorInterface
	blockReaderWriter blockReaderWriter
}

// AutoAssign automatically assigns one or more IP addresses as specified by the
// provided AutoAssignArgs.  AutoAssign returns the list of the assigned IPv4 addresses,
// and the list of the assigned IPv6 addresses.
func (c ipamClient) AutoAssign(ctx context.Context, args AutoAssignArgs) ([]net.IP, []net.IP, error) {
	// Determine the hostname to use - prefer the provided hostname if
	// non-nil, otherwise use the hostname reported by os.
	hostname, err := decideHostname(args.Hostname)
	if err != nil {
		return nil, nil, err
	}
	log.Infof("Auto-assign %d ipv4, %d ipv6 addrs for host '%s'", args.Num4, args.Num6, hostname)

	var v4list, v6list []net.IP

	if args.Num4 != 0 {
		// Assign IPv4 addresses.
		log.Debugf("Assigning IPv4 addresses")
		for _, pool := range args.IPv4Pools {
			if pool.IP.To4() == nil {
				return nil, nil, fmt.Errorf("provided IPv4 IPPools list contains one or more IPv6 IPPools")
			}
		}
		v4list, err = c.autoAssign(ctx, args.Num4, args.HandleID, args.Attrs, args.IPv4Pools, ipv4, hostname)
		if err != nil {
			log.Errorf("Error assigning IPV4 addresses: %v", err)
			return nil, nil, err
		}
	}

	if args.Num6 != 0 {
		// If no err assigning V4, try to assign any V6.
		log.Debugf("Assigning IPv6 addresses")
		for _, pool := range args.IPv6Pools {
			if pool.IP.To4() != nil {
				return nil, nil, fmt.Errorf("provided IPv6 IPPools list contains one or more IPv4 IPPools")
			}
		}
		v6list, err = c.autoAssign(ctx, args.Num6, args.HandleID, args.Attrs, args.IPv6Pools, ipv6, hostname)
		if err != nil {
			log.Errorf("Error assigning IPV6 addresses: %v", err)
			return nil, nil, err
		}
	}

	return v4list, v6list, nil
}

// getBlockFromAffinity returns the block referenced by the given affinity, attempting to create it if
// it does not exist. getBlockFromAffinity will delete the provided affinity if it does not match the actual
// affinity of the block.
func (c ipamClient) getBlockFromAffinity(ctx context.Context, aff *model.KVPair) (*model.KVPair, error) {
	// Parse out affinity data.
	cidr := aff.Key.(model.BlockAffinityKey).CIDR
	host := aff.Key.(model.BlockAffinityKey).Host
	state := aff.Value.(*model.BlockAffinity).State
	logCtx := log.WithFields(log.Fields{"cidr": cidr, "host": host, "state": state})

	// Get the block referenced by this affinity.
	b, err := c.client.Get(ctx, model.BlockKey{CIDR: cidr}, "")
	if err != nil {
		if _, ok := err.(cerrors.ErrorResourceDoesNotExist); ok {
			// The block referenced by the affinity doesn't exist. Try to create it.
			aff.Value.(*model.BlockAffinity).State = model.StatePending
			aff, err = c.client.Update(ctx, aff)
			if err != nil {
				logCtx.WithError(err).Debug("Error updating block affinity")
				return nil, err
			}

			cfg, err := c.GetIPAMConfig(ctx)
			if err != nil {
				logCtx.WithError(err).Errorf("Error getting IPAM Config")
				return nil, err
			}

			// Claim the block, which will also confirm the affinity.
			b, err := c.blockReaderWriter.claimAffineBlock(ctx, aff, *cfg)
			if err != nil {
				logCtx.WithError(err).Debug("Error claiming block affinity")
				return nil, err
			}
			return b, nil
		}
		logCtx.WithError(err).Error("Error getting block")
		return nil, err
	}

	// If the block doesn't match the affinity, it means we've got a stale affininty hanging around.
	// We should remove it.
	blockAffinity := b.Value.(*model.AllocationBlock).Affinity
	if blockAffinity == nil || *blockAffinity != fmt.Sprintf("host:%s", host) {
		logCtx.WithField("blockAffinity", blockAffinity).Warn("Block does not match the provided affinity, deleting stale affinity")
		_, err := c.client.Delete(ctx, aff.Key, aff.Revision)
		if err != nil {
			logCtx.WithError(err).Warn("Error deleting stale affinity")
			return nil, err
		}
		return nil, errStaleAffinity(fmt.Sprintf("Affinity is stale: %+v", aff))
	}

	// If the block does match the affinity but the affinity has not been confirmed,
	// try to confirm it. Treat empty string as confirmed for compatibility with older data.
	if state != model.StateConfirmed || state != "" {
		// Write the affinity as pending.
		aff.Value.(*model.BlockAffinity).State = model.StatePending
		aff, err = c.client.Update(ctx, aff)
		if err != nil {
			logCtx.WithError(err).Debug("Error marking affinity as pending")
			return nil, err
		}

		// CAS the block to get a new revision and invalidate any other instances
		// that might be trying to operate on the block.
		b, err = c.client.Update(ctx, b)
		if err != nil {
			logCtx.WithError(err).Debug("Error writing block")
			return nil, err
		}

		// Confirm the affinity.
		aff.Value.(*model.BlockAffinity).State = model.StateConfirmed
		aff, err = c.client.Update(ctx, aff)
		if err != nil {
			logCtx.WithError(err).Debug("Error confirming affinity")
			return nil, err
		}
	}
	return b, nil
}

func (c ipamClient) autoAssign(ctx context.Context, num int, handleID *string, attrs map[string]string, pools []net.IPNet, version ipVersion, host string) ([]net.IP, error) {

	// Start by trying to assign from one of the host-affine blocks.  We
	// always do strict checking at this stage, so it doesn't matter whether
	// globally we have strict_affinity or not.
	logCtx := log.WithFields(log.Fields{"host": host, "handle": handleID})
	logCtx.Debugf("Looking for addresses in current affine blocks for host")
	affBlocks, err := c.blockReaderWriter.getAffineBlocks(ctx, host, version, pools)
	if err != nil {
		return nil, err
	}
	logCtx.Debugf("Found %d affine IPv%d blocks for host: %v", len(affBlocks), version.Number, affBlocks)
	ips := []net.IP{}
	newIPs := []net.IP{}

	for len(ips) < num {
		if len(affBlocks) == 0 {
			logCtx.Infof("Ran out of existing affine blocks for host")
			break
		}
		cidr := affBlocks[0]
		affBlocks = affBlocks[1:]

		// Try to assign from this block - if we hit a CAS error, we'll try this block again.
		// For any other error, we'll break out and try the next affine block.
		for i := 0; i < ipamEtcdRetries; i++ {
			// Get the affinity.
			aff, err := c.client.Get(ctx, model.BlockAffinityKey{Host: host, CIDR: cidr}, "")
			if err != nil {
				logCtx.WithError(err).Warnf("Error getting affinity")
				break
			}

			// Get the block which is referenced by the affinity, creating it if necessary.
			b, err := c.getBlockFromAffinity(ctx, aff)
			if err != nil {
				// Couldn't get a block for this affinity.
				if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
					logCtx.WithError(err).Debug("CAS error getting affine block - retry")
					continue
				}
				logCtx.WithError(err).Warn("Couldn't get block for affinity, try next one")
				break
			}

			// Assign IPs from the block.
			logCtx.Info("Assigning from existing affine block")
			newIPs, err = c.assignFromExistingBlock(ctx, b, num, handleID, attrs, host, true)
			if err != nil {
				logCtx.WithError(err).Info("Couldn't assign from existing block")
				if _, ok := err.(cerrors.ErrorResourceUpdateConflict); !ok {
					logCtx.WithError(err).Debug("CAS error assigning from affine block - retry")
					continue
				}
				logCtx.WithError(err).Warn("Couldn't assign from affine block, try next one")
				break
			}
			ips = append(ips, newIPs...)
			break
		}
		logCtx.Debugf("Block '%s' provided addresses: %v", cidr.String(), newIPs)
	}

	// If there are still addresses to allocate, then we've run out of
	// blocks with affinity.  Before we can assign new blocks or assign in
	// non-affine blocks, we need to check that our IPAM configuration
	// allows that.
	config, err := c.GetIPAMConfig(ctx)
	if err != nil {
		return ips, err
	}
	logCtx.Debugf("Allocate new blocks? Config: %+v", config)
	if config.AutoAllocateBlocks == true {
		rem := num - len(ips)
		retries := ipamEtcdRetries
		for rem > 0 && retries > 0 {
			// Claim a new block.
			logCtx.Infof("Need to allocate %d more addresses - allocate another block", rem)
			retries = retries - 1

			// First, try to find an unclaimed block.
			subnet, err := c.blockReaderWriter.findUnclaimedBlock(ctx, host, version, pools, *config)
			if err != nil {
				if _, ok := err.(noFreeBlocksError); ok {
					// No free blocks.  Break.
					logCtx.Info("No free blocks available for allocation")
					break
				}
				log.WithError(err).Error("Failed to find an unclaimed block")
				return ips, err
			}
			logCtx := log.WithFields(log.Fields{"host": host, "subnet": subnet})

			for i := 0; i < ipamEtcdRetries; i++ {
				// We found an unclaimed block - claim affinity for it.
				pa, err := c.blockReaderWriter.getPendingAffinity(ctx, host, *subnet)
				if err != nil {
					if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
						logCtx.WithError(err).Debug("CAS error claiming pending affinity, retry")
						continue
					}
					logCtx.WithError(err).Errorf("Error claiming pending affinity")
					return ips, err
				}
				logCtx.Debugf("Claimed pending affinity for %+v", pa.Value)

				// We have a pending affinity - try to get the block.
				b, err := c.getBlockFromAffinity(ctx, pa)
				if err != nil {
					if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
						logCtx.WithError(err).Debug("CAS error getting block, retry")
						continue
					} else if _, ok := err.(errBlockClaimConflict); ok {
						logCtx.WithError(err).Debug("Block taken by someone else, find a new one")
						break
					} else if _, ok := err.(errStaleAffinity); ok {
						logCtx.WithError(err).Debug("Affinity is stale, find a new one")
						break
					}
					logCtx.WithError(err).Errorf("Error getting block for affinity")
					return ips, err
				}

				// Claim successful.  Assign addresses from the new block.
				logCtx.Infof("Claimed new block %v - assigning %d addresses", b, rem)
				newIPs, err := c.assignFromExistingBlock(ctx, b, rem, handleID, attrs, host, config.StrictAffinity)
				if err != nil {
					if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
						log.WithError(err).Debug("CAS Error assigning from new block - retry")
						continue
					}
					logCtx.WithError(err).Warningf("Failed to assign IPs in newly allocated block")
					break
				}
				logCtx.Debugf("Assigned IPs from new block: %s", newIPs)
				ips = append(ips, newIPs...)
				rem = num - len(ips)
				break
			}
		}

		if retries == 0 {
			return ips, errors.New("Max retries hit - excessive concurrent IPAM requests")
		}
	}

	// If there are still addresses to allocate, we've now tried all blocks
	// with some affinity to us, and tried (and failed) to allocate new
	// ones.  If we do not require strict host affinity, our last option is
	// a random hunt through any blocks we haven't yet tried.
	//
	// Note that this processing simply takes all of the IP pools and breaks
	// them up into block-sized CIDRs, then shuffles and searches through each
	// CIDR.  This algorithm does not work if we disallow auto-allocation of
	// blocks because the allocated blocks may be sparsely populated in the
	// pools resulting in a very slow search for free addresses.
	//
	// If we need to support non-strict affinity and no auto-allocation of
	// blocks, then we should query the actual allocation blocks and assign
	// from those.
	rem := num - len(ips)
	if config.StrictAffinity != true && rem != 0 {
		logCtx.Infof("Attempting to assign %d more addresses from non-affine blocks", rem)
		// Figure out the pools to allocate from.
		if len(pools) == 0 {
			// Default to all configured pools.
			pools, err = c.pools.GetEnabledPools(version.Number)
			if err != nil {
				logCtx.Errorf("Error reading configured pools: %v", err)
				return ips, nil
			}
		}

		// Iterate over pools and assign addresses until we either run out of pools,
		// or the request has been satisfied.
		for _, p := range pools {
			logCtx.Debugf("Assigning from random blocks in pool %s", p.String())
			newBlock := randomBlockGenerator(p, host)
			for rem > 0 {
				// Grab a new random block.
				blockCIDR := newBlock()
				if blockCIDR == nil {
					logCtx.Warningf("All addresses exhausted in pool %s", p.String())
					break
				}

				for i := 0; i < ipamEtcdRetries; i++ {
					b, err := c.client.Get(ctx, model.BlockKey{CIDR: *blockCIDR}, "")
					if err != nil {
						logCtx.WithError(err).Warn("Failed to get non-affine block")
						break
					}

					// Attempt to assign from the block.
					newIPs, err := c.assignFromExistingBlock(ctx, b, rem, handleID, attrs, host, false)
					if err != nil {
						if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
							logCtx.WithError(err).Debug("CAS error assigning from non-affine block - retry")
							continue
						}
						logCtx.WithError(err).Warningf("Failed to assign IPs from non-affine block in pool %s", p.String())
						break
					}
					ips = append(ips, newIPs...)
					rem = num - len(ips)
					break
				}
			}
		}
	}

	logCtx.Infof("Auto-assigned %d out of %d IPv%ds: %v", len(ips), num, version.Number, ips)
	return ips, nil
}

// AssignIP assigns the provided IP address to the provided host.  The IP address
// must fall within a configured pool.  AssignIP will claim block affinity as needed
// in order to satisfy the assignment.  An error will be returned if the IP address
// is already assigned, or if StrictAffinity is enabled and the address is within
// a block that does not have affinity for the given host.
func (c ipamClient) AssignIP(ctx context.Context, args AssignIPArgs) error {
	hostname, err := decideHostname(args.Hostname)
	if err != nil {
		return err
	}
	log.Infof("Assigning IP %s to host: %s", args.IP, hostname)

	if !c.blockReaderWriter.withinConfiguredPools(args.IP) {
		return errors.New("The provided IP address is not in a configured pool\n")
	}

	blockCIDR := getBlockCIDRForAddress(args.IP)
	log.Debugf("IP %s is in block '%s'", args.IP.String(), blockCIDR.String())
	for i := 0; i < ipamEtcdRetries; i++ {
		obj, err := c.client.Get(ctx, model.BlockKey{blockCIDR}, "")
		if err != nil {
			if _, ok := err.(cerrors.ErrorResourceDoesNotExist); !ok {
				log.WithError(err).Error("Error getting block")
				return err
			}

			// Block doesn't exist, we need to create it.  First,
			// validate the given IP address is within a configured pool.
			if !c.blockReaderWriter.withinConfiguredPools(args.IP) {
				estr := fmt.Sprintf("The given IP address (%s) is not in any configured pools", args.IP.String())
				log.Errorf(estr)
				return errors.New(estr)
			}

			log.Debugf("Block for IP %s does not yet exist, creating", args.IP)
			cfg, err := c.GetIPAMConfig(ctx)
			if err != nil {
				log.Errorf("Error getting IPAM Config: %v", err)
				return err
			}

			pa, err := c.blockReaderWriter.getPendingAffinity(ctx, hostname, blockCIDR)
			if err != nil {
				if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
					log.WithError(err).Debug("CAS error claiming affinity for block - retry")
					continue
				}
				return err
			}

			obj, err = c.blockReaderWriter.claimAffineBlock(ctx, pa, *cfg)
			if err != nil {
				if _, ok := err.(*errBlockClaimConflict); ok {
					log.Warningf("Someone else claimed block %s before us", blockCIDR.String())
					continue
				} else if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
					log.WithError(err).Debug("CAS error claiming affine block - retry")
					continue
				}
				log.WithError(err).Error("Error claiming block")
				return err
			}
			log.Infof("Claimed new block: %s", blockCIDR)
		}

		block := allocationBlock{obj.Value.(*model.AllocationBlock)}
		err = block.assign(args.IP, args.HandleID, args.Attrs, hostname)
		if err != nil {
			log.Errorf("Failed to assign address %v: %v", args.IP, err)
			return err
		}

		// Increment handle.
		if args.HandleID != nil {
			c.incrementHandle(ctx, *args.HandleID, blockCIDR, 1)
		}

		// Update the block using the original KVPair to do a CAS.  No need to
		// update the Value since we have been manipulating the Value pointed to
		// in the KVPair.
		_, err = c.client.Update(ctx, obj)
		if err != nil {
			if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
				log.WithError(err).Debug("CAS error assigning IP - retry")
				continue
			}

			log.WithError(err).Warningf("Update failed on block %s", block.CIDR.String())
			if args.HandleID != nil {
				if err := c.decrementHandle(ctx, *args.HandleID, blockCIDR, 1); err != nil {
					log.WithError(err).Warn("Failed to decrement handle")
				}
			}
			return err
		}
		return nil
	}
	return errors.New("Max retries hit - excessive concurrent IPAM requests")
}

// ReleaseIPs releases any of the given IP addresses that are currently assigned,
// so that they are available to be used in another assignment.
func (c ipamClient) ReleaseIPs(ctx context.Context, ips []net.IP) ([]net.IP, error) {
	log.Infof("Releasing IP addresses: %v", ips)
	unallocated := []net.IP{}

	// Group IP addresses by block to minimize the number of writes
	// to the datastore required to release the given addresses.
	ipsByBlock := map[string][]net.IP{}
	for _, ip := range ips {
		// Check if we've already got an entry for this block.
		blockCIDR := getBlockCIDRForAddress(ip)
		cidrStr := blockCIDR.String()
		if _, exists := ipsByBlock[cidrStr]; !exists {
			// Entry does not exist, create it.
			ipsByBlock[cidrStr] = []net.IP{}
		}

		// Append to the list.
		ipsByBlock[cidrStr] = append(ipsByBlock[cidrStr], ip)
	}

	// Release IPs for each block.
	for cidrStr, ips := range ipsByBlock {
		_, cidr, _ := net.ParseCIDR(cidrStr)
		unalloc, err := c.releaseIPsFromBlock(ctx, ips, *cidr)
		if err != nil {
			log.Errorf("Error releasing IPs: %v", err)
			return nil, err
		}
		unallocated = append(unallocated, unalloc...)
	}
	return unallocated, nil
}

func (c ipamClient) releaseIPsFromBlock(ctx context.Context, ips []net.IP, blockCIDR net.IPNet) ([]net.IP, error) {
	for i := 0; i < ipamEtcdRetries; i++ {
		obj, err := c.client.Get(ctx, model.BlockKey{CIDR: blockCIDR}, "")
		if err != nil {
			if _, ok := err.(cerrors.ErrorResourceDoesNotExist); ok {
				// The block does not exist - all addresses must be unassigned.
				return ips, nil
			} else {
				// Unexpected error reading block.
				return nil, err
			}
		}

		// Block exists - get the allocationBlock from the KVPair.
		b := allocationBlock{obj.Value.(*model.AllocationBlock)}

		// Release the IPs.
		unallocated, handles, err2 := b.release(ips)
		if err2 != nil {
			return nil, err2
		}
		if len(ips) == len(unallocated) {
			// All the given IP addresses are already unallocated.
			// Just return.
			return unallocated, nil
		}

		// If the block is empty and has no affinity, we can delete it.
		// Otherwise, update the block using CAS.  There is no need to update
		// the Value since we have updated the structure pointed to in the
		// KVPair.
		var updateErr error
		if b.empty() && b.Affinity == nil {
			log.Debugf("Deleting non-affine block '%s'", b.CIDR.String())
			_, updateErr = c.client.Delete(ctx, obj.Key, obj.Revision)
		} else {
			log.Debugf("Updating assignments in block '%s'", b.CIDR.String())
			_, updateErr = c.client.Update(ctx, obj)
		}

		if updateErr != nil {
			if _, ok := updateErr.(cerrors.ErrorResourceUpdateConflict); ok {
				// Comparison error - retry.
				log.Warningf("Failed to update block '%s' - retry #%d", b.CIDR.String(), i)
				continue
			} else {
				// Something else - return the error.
				log.Errorf("Error updating block '%s': %v", b.CIDR.String(), updateErr)
				return nil, updateErr
			}
		}

		// Success - decrement handles.
		log.Debugf("Decrementing handles: %v", handles)
		for handleID, amount := range handles {
			c.decrementHandle(ctx, handleID, blockCIDR, amount)
		}
		return unallocated, nil
	}
	return nil, errors.New("Max retries hit - excessive concurrent IPAM requests")
}

func (c ipamClient) assignFromExistingBlock(ctx context.Context, block *model.KVPair, num int, handleID *string, attrs map[string]string, host string, affCheck bool) ([]net.IP, error) {
	var ips []net.IP
	blockCIDR := block.Key.(model.BlockKey).CIDR
	logCtx := log.WithFields(log.Fields{"host": host, "handle": handleID, "block": blockCIDR})
	logCtx.Debugf("Attempting to assign %d addresses from block", num)

	// Pull out the block.
	b := allocationBlock{block.Value.(*model.AllocationBlock)}

	logCtx.Debugf("Got block: %+v", b)
	var err error
	ips, err = b.autoAssign(num, handleID, host, attrs, affCheck)
	if err != nil {
		logCtx.WithError(err).Errorf("Error in auto assign")
		return nil, err
	}
	if len(ips) == 0 {
		logCtx.Infof("Block is full")
		return []net.IP{}, nil
	}

	// Increment handle count.
	if handleID != nil {
		c.incrementHandle(ctx, *handleID, blockCIDR, num)
	}

	// Update the block using CAS by passing back the original
	// KVPair.
	block.Value = b.AllocationBlock
	_, err = c.client.Update(ctx, block)
	if err != nil {
		logCtx.WithError(err).Infof("Failed to update block")
		if handleID != nil {
			c.decrementHandle(ctx, *handleID, blockCIDR, num)
		}
		return nil, err
	}
	return ips, nil
}

// ClaimAffinity makes a best effort to claim affinity to the given host for all blocks
// within the given CIDR.  The given CIDR must fall within a configured
// pool.  Returns a list of blocks that were claimed, as well as a
// list of blocks that were claimed by another host.
// If an empty string is passed as the host, then the hostname is automatically detected.
func (c ipamClient) ClaimAffinity(ctx context.Context, cidr net.IPNet, host string) ([]net.IPNet, []net.IPNet, error) {
	logCtx := log.WithFields(log.Fields{"host": host})

	// Validate that the given CIDR is at least as big as a block.
	if !largerThanOrEqualToBlock(cidr) {
		estr := fmt.Sprintf("The requested CIDR (%s) is smaller than the minimum.", cidr.String())
		return nil, nil, invalidSizeError(estr)
	}

	// Determine the hostname to use.
	hostname, err := decideHostname(host)
	if err != nil {
		return nil, nil, err
	}
	failed := []net.IPNet{}
	claimed := []net.IPNet{}

	// Verify the requested CIDR falls within a configured pool.
	if !c.blockReaderWriter.withinConfiguredPools(net.IP{cidr.IP}) {
		estr := fmt.Sprintf("The requested CIDR (%s) is not within any configured pools.", cidr.String())
		return nil, nil, errors.New(estr)
	}

	// Get IPAM config.
	cfg, err := c.GetIPAMConfig(ctx)
	if err != nil {
		logCtx.Errorf("Failed to get IPAM Config: %v", err)
		return nil, nil, err
	}

	// Claim all blocks within the given cidr.
	blocks := blockGenerator(cidr)
	for blockCIDR := blocks(); blockCIDR != nil; blockCIDR = blocks() {
		for i := 0; i < ipamEtcdRetries; i++ {
			// First, claim a pending affinity.
			pa, err := c.blockReaderWriter.getPendingAffinity(ctx, hostname, *blockCIDR)
			if err != nil {
				if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
					logCtx.WithError(err).Debug("CAS error getting pending affinity - retry")
					continue
				}
				return claimed, failed, err
			}

			// Once we have the affinity, claim the block, which will confirm the affinity.
			_, err = c.blockReaderWriter.claimAffineBlock(ctx, pa, *cfg)
			if err != nil {
				if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
					logCtx.WithError(err).Debug("CAS error claiming affine block - retry")
					continue
				} else if _, ok := err.(errBlockClaimConflict); ok {
					logCtx.Debugf("Block %s is claimed by another host", blockCIDR.String())
					failed = append(failed, *blockCIDR)
				} else {
					logCtx.Errorf("Failed to claim block: %v", err)
					return claimed, failed, err
				}
			} else {
				logCtx.Debugf("Claimed CIDR %s", blockCIDR.String())
				claimed = append(claimed, *blockCIDR)
			}
			break
		}
	}
	return claimed, failed, nil
}

// ReleaseAffinity releases affinity for all blocks within the given CIDR
// on the given host.  If a block does not have affinity for the given host,
// its affinity will not be released and no error will be returned.
// If an empty string is passed as the host, then the hostname is automatically detected.
func (c ipamClient) ReleaseAffinity(ctx context.Context, cidr net.IPNet, host string) error {
	// Validate that the given CIDR is at least as big as a block.
	if !largerThanOrEqualToBlock(cidr) {
		estr := fmt.Sprintf("The requested CIDR (%s) is smaller than the minimum.", cidr.String())
		return invalidSizeError(estr)
	}

	// Determine the hostname to use.
	hostname, err := decideHostname(host)
	if err != nil {
		return err
	}

	// Release all blocks within the given cidr.
	blocks := blockGenerator(cidr)
	for blockCIDR := blocks(); blockCIDR != nil; blockCIDR = blocks() {
		err := c.blockReaderWriter.releaseBlockAffinity(ctx, hostname, *blockCIDR)
		if err != nil {
			if _, ok := err.(errBlockClaimConflict); ok {
				// Not claimed by this host - ignore.
			} else if _, ok := err.(cerrors.ErrorResourceDoesNotExist); ok {
				// Block does not exist - ignore.
			} else {
				log.Errorf("Error releasing affinity for '%s': %v", *blockCIDR, err)
				return err
			}
		}
	}
	return nil
}

// ReleaseHostAffinities releases affinity for all blocks that are affine
// to the given host.  If an empty string is passed as the host,
// then the hostname is automatically detected.
func (c ipamClient) ReleaseHostAffinities(ctx context.Context, host string) error {
	hostname, err := decideHostname(host)
	if err != nil {
		return err
	}

	versions := []ipVersion{ipv4, ipv6}
	for _, version := range versions {
		blockCIDRs, err := c.blockReaderWriter.getAffineBlocks(ctx, hostname, version, nil)
		if err != nil {
			return err
		}

		for _, blockCIDR := range blockCIDRs {
			err := c.ReleaseAffinity(ctx, blockCIDR, hostname)
			if err != nil {
				if _, ok := err.(errBlockClaimConflict); ok {
					// Claimed by a different host.
				} else {
					return err
				}
			}
		}
	}
	return nil
}

// ReleasePoolAffinities releases affinity for all blocks within
// the specified pool across all hosts.
func (c ipamClient) ReleasePoolAffinities(ctx context.Context, pool net.IPNet) error {
	log.Infof("Releasing block affinities within pool '%s'", pool.String())
	for i := 0; i < ipamKeyErrRetries; i++ {
		retry := false
		pairs, err := c.hostBlockPairs(ctx, pool)
		if err != nil {
			return err
		}

		if len(pairs) == 0 {
			log.Debugf("No blocks have affinity")
			return nil
		}

		for blockString, host := range pairs {
			_, blockCIDR, _ := net.ParseCIDR(blockString)
			err = c.blockReaderWriter.releaseBlockAffinity(ctx, host, *blockCIDR)
			if err != nil {
				if _, ok := err.(errBlockClaimConflict); ok {
					retry = true
				} else if _, ok := err.(cerrors.ErrorResourceDoesNotExist); ok {
					log.Debugf("No such block '%s'", blockCIDR.String())
					continue
				} else {
					log.Errorf("Error releasing affinity for '%s': %v", blockCIDR.String(), err)
					return err
				}
			}

		}

		if !retry {
			return nil
		}
	}
	return errors.New("Max retries hit - excessive concurrent IPAM requests")
}

// RemoveIPAMHost releases affinity for all blocks on the given host,
// and removes all host-specific IPAM data from the datastore.
// RemoveIPAMHost does not release any IP addresses claimed on the given host.
// If an empty string is passed as the host, then the hostname is automatically detected.
func (c ipamClient) RemoveIPAMHost(ctx context.Context, host string) error {
	// Determine the hostname to use.
	hostname, err := decideHostname(host)
	if err != nil {
		return err
	}

	// Release affinities for this host.
	if err := c.ReleaseHostAffinities(ctx, hostname); err != nil {
		log.WithError(err).Errorf("Failed to release IPAM affinities for host")
		return err
	}

	for i := 0; i < ipamEtcdRetries; i++ {
		// Get the IPAM host.
		k := model.IPAMHostKey{Host: hostname}
		kvp, err := c.client.Get(ctx, k, "")
		if err != nil {
			log.WithError(err).Errorf("Failed to get IPAM host")
			return err
		}

		// Remove the host tree from the datastore.
		_, err = c.client.Delete(ctx, k, kvp.Revision)
		if err != nil {
			if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
				// We hit a compare-and-delete error - retry.
				continue
			}

			// Return the error unless the resource does not exist.
			if _, ok := err.(cerrors.ErrorResourceDoesNotExist); !ok {
				log.Errorf("Error removing IPAM host: %v", err)
				return err
			}
		}
		return nil
	}

	return errors.New("Max retries hit")
}

func (c ipamClient) hostBlockPairs(ctx context.Context, pool net.IPNet) (map[string]string, error) {
	pairs := map[string]string{}

	// Get all blocks and their affinities.
	objs, err := c.client.List(ctx, model.BlockAffinityListOptions{}, "")
	if err != nil {
		log.Errorf("Error querying block affinities: %v", err)
		return nil, err
	}

	// Iterate through each block affinity and build up a mapping
	// of blockCidr -> host.
	log.Debugf("Getting block -> host mappings")
	for _, o := range objs.KVPairs {
		k := o.Key.(model.BlockAffinityKey)

		// Only add the pair to the map if the block belongs to the pool.
		if pool.Contains(k.CIDR.IPNet.IP) {
			pairs[k.CIDR.String()] = k.Host
		}
		log.Debugf("Block %s -> %s", k.CIDR.String(), k.Host)
	}

	return pairs, nil
}

// IpsByHandle returns a list of all IP addresses that have been
// assigned using the provided handle.
func (c ipamClient) IPsByHandle(ctx context.Context, handleID string) ([]net.IP, error) {
	obj, err := c.client.Get(ctx, model.IPAMHandleKey{HandleID: handleID}, "")
	if err != nil {
		return nil, err
	}
	handle := allocationHandle{obj.Value.(*model.IPAMHandle)}

	assignments := []net.IP{}
	for k, _ := range handle.Block {
		_, blockCIDR, _ := net.ParseCIDR(k)
		obj, err := c.client.Get(ctx, model.BlockKey{*blockCIDR}, "")
		if err != nil {
			log.Warningf("Couldn't read block %s referenced by handle %s", blockCIDR, handleID)
			continue
		}

		// Pull out the allocationBlock and get all the assignments
		// from it.
		b := allocationBlock{obj.Value.(*model.AllocationBlock)}
		assignments = append(assignments, b.ipsByHandle(handleID)...)
	}
	return assignments, nil
}

// ReleaseByHandle releases all IP addresses that have been assigned
// using the provided handle.
func (c ipamClient) ReleaseByHandle(ctx context.Context, handleID string) error {
	log.Infof("Releasing all IPs with handle '%s'", handleID)
	obj, err := c.client.Get(ctx, model.IPAMHandleKey{HandleID: handleID}, "")
	if err != nil {
		return err
	}
	handle := allocationHandle{obj.Value.(*model.IPAMHandle)}

	for blockStr, _ := range handle.Block {
		_, blockCIDR, _ := net.ParseCIDR(blockStr)
		err = c.releaseByHandle(ctx, handleID, *blockCIDR)
	}
	return nil
}

func (c ipamClient) releaseByHandle(ctx context.Context, handleID string, blockCIDR net.IPNet) error {
	for i := 0; i < ipamEtcdRetries; i++ {
		obj, err := c.client.Get(ctx, model.BlockKey{CIDR: blockCIDR}, "")
		if err != nil {
			if _, ok := err.(cerrors.ErrorResourceDoesNotExist); ok {
				// Block doesn't exist, so all addresses are already
				// unallocated.  This can happen when a handle is
				// overestimating the number of assigned addresses.
				return nil
			} else {
				return err
			}
		}
		block := allocationBlock{obj.Value.(*model.AllocationBlock)}
		num := block.releaseByHandle(handleID)
		if num == 0 {
			// Block has no addresses with this handle, so
			// all addresses are already unallocated.
			return nil
		}

		if block.empty() && block.Affinity == nil {
			_, err = c.client.Delete(ctx, model.BlockKey{blockCIDR}, obj.Revision)
			if err != nil {
				// Return the error unless the resource does not exist.
				if _, ok := err.(cerrors.ErrorResourceDoesNotExist); !ok {
					log.Errorf("Error deleting block: %v", err)
					return err
				}
			}
		} else {
			// Compare and swap the AllocationBlock using the original
			// KVPair read from before.  No need to update the Value since we
			// have been directly manipulating the value referenced by the KVPair.
			_, err = c.client.Update(ctx, obj)
			if err != nil {
				if _, ok := err.(cerrors.ErrorResourceUpdateConflict); ok {
					// Comparison failed - retry.
					log.Warningf("CAS error for block, retry #%d: %v", i, err)
					continue
				} else {
					// Something else - return the error.
					log.Errorf("Error updating block '%s': %v", block.CIDR.String(), err)
					return err
				}
			}
		}

		c.decrementHandle(ctx, handleID, blockCIDR, num)
		return nil
	}
	return errors.New("Hit max retries")
}

func (c ipamClient) incrementHandle(ctx context.Context, handleID string, blockCIDR net.IPNet, num int) error {
	var obj *model.KVPair
	var err error
	for i := 0; i < ipamEtcdRetries; i++ {
		obj, err = c.client.Get(ctx, model.IPAMHandleKey{HandleID: handleID}, "")
		if err != nil {
			if _, ok := err.(cerrors.ErrorResourceDoesNotExist); ok {
				// Handle doesn't exist - create it.
				log.Infof("Creating new handle: %s", handleID)
				bh := model.IPAMHandle{
					HandleID: handleID,
					Block:    map[string]int{},
				}
				obj = &model.KVPair{
					Key:   model.IPAMHandleKey{HandleID: handleID},
					Value: &bh,
				}
			} else {
				// Unexpected error reading handle.
				return err
			}
		}

		// Get the handle from the KVPair.
		handle := allocationHandle{obj.Value.(*model.IPAMHandle)}

		// Increment the handle for this block.
		handle.incrementBlock(blockCIDR, num)

		// Compare and swap the handle using the KVPair from above.  We've been
		// manipulating the structure in the KVPair, so pass straight back to
		// apply the changes.
		if obj.Revision != "" {
			// This is an existing handle - update it.
			_, err = c.client.Update(ctx, obj)
			if err != nil {
				log.WithError(err).Warning("Failed to update handle, retry")
				continue
			}
		} else {
			// This is a new handle - create it.
			_, err = c.client.Create(ctx, obj)
			if err != nil {
				log.WithError(err).Warning("Failed to create handle, retry")
				continue
			}
		}
		return nil
	}
	return errors.New("Max retries hit - excessive concurrent IPAM requests")

}

func (c ipamClient) decrementHandle(ctx context.Context, handleID string, blockCIDR net.IPNet, num int) error {
	for i := 0; i < ipamEtcdRetries; i++ {
		obj, err := c.client.Get(ctx, model.IPAMHandleKey{HandleID: handleID}, "")
		if err != nil {
			return fmt.Errorf("Can't decrement block with handle '%+v' because it doesn't exist", handleID)
		}
		handle := allocationHandle{obj.Value.(*model.IPAMHandle)}

		_, err = handle.decrementBlock(blockCIDR, num)
		if err != nil {
			return fmt.Errorf("Can't decrement block with handle '%+v': too few allocated", handleID)
		}

		// Update / Delete as appropriate.  Since we have been manipulating the
		// data in the KVPair, just pass this straight back to the client.
		if handle.empty() {
			log.Debugf("Deleting handle: %s", handleID)
			_, err = c.client.Delete(ctx, obj.Key, obj.Revision)
		} else {
			log.Debugf("Updating handle: %s", handleID)
			_, err = c.client.Update(ctx, obj)
		}

		// Check error.
		if err != nil {
			continue
		}
		log.Infof("Decremented handle '%s' by %d", handleID, num)
		return nil
	}
	return errors.New("Max retries hit - excessive concurrent IPAM requests")
}

// GetAssignmentAttributes returns the attributes stored with the given IP address
// upon assignment.
func (c ipamClient) GetAssignmentAttributes(ctx context.Context, addr net.IP) (map[string]string, error) {
	blockCIDR := getBlockCIDRForAddress(addr)
	obj, err := c.client.Get(ctx, model.BlockKey{blockCIDR}, "")
	if err != nil {
		log.Errorf("Error reading block %s: %v", blockCIDR, err)
		return nil, errors.New(fmt.Sprintf("%s is not assigned", addr))
	}
	block := allocationBlock{obj.Value.(*model.AllocationBlock)}
	return block.attributesForIP(addr)
}

// GetIPAMConfig returns the global IPAM configuration.  If no IPAM configuration
// has been set, returns a default configuration with StrictAffinity disabled
// and AutoAllocateBlocks enabled.
func (c ipamClient) GetIPAMConfig(ctx context.Context) (*IPAMConfig, error) {
	obj, err := c.client.Get(ctx, model.IPAMConfigKey{}, "")
	if err != nil {
		if _, ok := err.(cerrors.ErrorResourceDoesNotExist); ok {
			// IPAMConfig has not been explicitly set.  Return
			// a default IPAM configuration.
			return &IPAMConfig{AutoAllocateBlocks: true, StrictAffinity: false}, nil
		}
		log.Errorf("Error getting IPAMConfig: %v", err)
		return nil, err
	}
	return c.convertBackendToIPAMConfig(obj.Value.(*model.IPAMConfig)), nil
}

// SetIPAMConfig sets global IPAM configuration.  This can only
// be done when there are no allocated blocks and IP addresses.
func (c ipamClient) SetIPAMConfig(ctx context.Context, cfg IPAMConfig) error {
	current, err := c.GetIPAMConfig(ctx)
	if err != nil {
		return err
	}

	if *current == cfg {
		return nil
	}

	if !cfg.StrictAffinity && !cfg.AutoAllocateBlocks {
		return errors.New("Cannot disable 'StrictAffinity' and 'AutoAllocateBlocks' at the same time")
	}

	allObjs, err := c.client.List(ctx, model.BlockListOptions{}, "")
	if len(allObjs.KVPairs) != 0 {
		return errors.New("Cannot change IPAM config while allocations exist")
	}

	// Write to datastore.
	obj := model.KVPair{
		Key:   model.IPAMConfigKey{},
		Value: c.convertIPAMConfigToBackend(&cfg),
	}
	_, err = c.client.Apply(ctx, &obj)
	if err != nil {
		log.Errorf("Error applying IPAMConfig: %v", err)
		return err
	}
	return nil
}

func (c ipamClient) convertIPAMConfigToBackend(cfg *IPAMConfig) *model.IPAMConfig {
	return &model.IPAMConfig{
		StrictAffinity:     cfg.StrictAffinity,
		AutoAllocateBlocks: cfg.AutoAllocateBlocks,
	}
}

func (c ipamClient) convertBackendToIPAMConfig(cfg *model.IPAMConfig) *IPAMConfig {
	return &IPAMConfig{
		StrictAffinity:     cfg.StrictAffinity,
		AutoAllocateBlocks: cfg.AutoAllocateBlocks,
	}
}

func decideHostname(host string) (string, error) {
	// Determine the hostname to use - prefer the provided hostname if
	// non-nil, otherwise use the hostname reported by os.
	var hostname string
	var err error
	if host != "" {
		hostname = host
	} else {
		hostname, err = names.Hostname()
		if err != nil {
			return "", fmt.Errorf("Failed to acquire hostname: %+v", err)
		}
	}
	log.Debugf("Using hostname=%s", hostname)
	return hostname, nil
}
