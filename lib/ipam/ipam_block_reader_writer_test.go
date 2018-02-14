// Copyright (c) 2016-2017 Tigera, Inc. All rights reserved.

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
	"fmt"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	log "github.com/sirupsen/logrus"

	"github.com/projectcalico/libcalico-go/lib/apiconfig"
	"github.com/projectcalico/libcalico-go/lib/backend"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	cerrors "github.com/projectcalico/libcalico-go/lib/errors"
	cnet "github.com/projectcalico/libcalico-go/lib/net"
	"github.com/projectcalico/libcalico-go/lib/testutils"
)

func newFakeClient() *fakeClient {
	return &fakeClient{
		createFuncs: map[string]func(ctx context.Context, object *model.KVPair) (*model.KVPair, error){},
		getFuncs:    map[string]func(ctx context.Context, key model.Key, revision string) (*model.KVPair, error){},
		deleteFuncs: map[string]func(ctx context.Context, key model.Key, revision string) (*model.KVPair, error){},
		listFuncs:   map[string]func(ctx context.Context, list model.ListInterface, revision string) (*model.KVPairList, error){},
	}
}

// fakeClient implements the backend api.Client interface.
type fakeClient struct {
	createFuncs map[string]func(ctx context.Context, object *model.KVPair) (*model.KVPair, error)
	getFuncs    map[string]func(ctx context.Context, key model.Key, revision string) (*model.KVPair, error)
	deleteFuncs map[string]func(ctx context.Context, key model.Key, revision string) (*model.KVPair, error)
	listFuncs   map[string]func(ctx context.Context, list model.ListInterface, revision string) (*model.KVPairList, error)
}

// We don't implement any of the CRUD related methods, just the Watch method to return
// a fake watcher that the test code will drive.
func (c *fakeClient) Create(ctx context.Context, object *model.KVPair) (*model.KVPair, error) {
	if f, ok := c.createFuncs[fmt.Sprintf("%s", object.Key)]; ok {
		return f(ctx, object)
	}
	panic(fmt.Sprintf("Create called on unexpected object: %+v", object))
	return nil, nil
}
func (c *fakeClient) Update(ctx context.Context, object *model.KVPair) (*model.KVPair, error) {
	panic("should not be called")
	return nil, nil
}
func (c *fakeClient) Apply(ctx context.Context, object *model.KVPair) (*model.KVPair, error) {
	panic("should not be called")
	return nil, nil
}
func (c *fakeClient) Delete(ctx context.Context, key model.Key, revision string) (*model.KVPair, error) {
	if f, ok := c.deleteFuncs[fmt.Sprintf("%s", key)]; ok {
		return f(ctx, key, revision)
	}
	panic(fmt.Sprintf("Delete called on unexpected object: %+v", key))
	return nil, nil
}
func (c *fakeClient) Get(ctx context.Context, key model.Key, revision string) (*model.KVPair, error) {
	if f, ok := c.getFuncs[fmt.Sprintf("%s", key)]; ok {
		return f(ctx, key, revision)
	} else if f, ok := c.getFuncs["default"]; ok {
		return f(ctx, key, revision)
	}
	panic(fmt.Sprintf("Get called on unexpected object: %+v", key))
	return nil, nil
}
func (c *fakeClient) Syncer(callbacks api.SyncerCallbacks) api.Syncer {
	panic("should not be called")
	return nil
}
func (c *fakeClient) EnsureInitialized() error {
	panic("should not be called")
	return nil
}
func (c *fakeClient) Clean() error {
	panic("should not be called")
	return nil
}

func (c *fakeClient) List(ctx context.Context, list model.ListInterface, revision string) (*model.KVPairList, error) {
	if f, ok := c.listFuncs[fmt.Sprintf("%s", list)]; ok {
		return f(ctx, list, revision)
	} else if f, ok := c.listFuncs["default"]; ok {
		return f(ctx, list, revision)
	}
	panic(fmt.Sprintf("List called on unexpected object: %+v", list))
	return nil, nil
}

func (c *fakeClient) Watch(ctx context.Context, list model.ListInterface, revision string) (api.WatchInterface, error) {
	panic("should not be called")
	return nil, nil
}

var _ = testutils.E2eDatastoreDescribe("IPAM block allocation tests", testutils.DatastoreEtcdV3, func(config apiconfig.CalicoAPIConfig) {

	ctx := context.Background()
	log.SetLevel(log.DebugLevel)

	Context("IPAM block allocation race conditions (mocked client)", func() {
		It("should support multiple async block affinity claims on the same block", func() {
			var bc api.Client
			By("creating a backend client", func() {
				var err error
				bc, err = backend.NewClient(config)
				Expect(err).NotTo(HaveOccurred())
			})

			host := "ipam-test-host"
			otherHost := "another-host"
			var net *cnet.IPNet
			By("picking a block cidr", func() {
				var err error
				_, net, err = cnet.ParseCIDR("10.0.0.0/26")
				Expect(err).NotTo(HaveOccurred())
			})

			// Configure a fake client such that we successfully create the
			// block affininty, but fail to create the actual block.
			fc := newFakeClient()

			// Creation function for a block affinity - actually create it.
			affKVP := &model.KVPair{
				Key:   model.BlockAffinityKey{Host: host, CIDR: *net},
				Value: model.BlockAffinity{},
			}
			affKVP2 := &model.KVPair{
				Key:   model.BlockAffinityKey{Host: otherHost, CIDR: *net},
				Value: model.BlockAffinity{},
			}

			fc.createFuncs[fmt.Sprintf("%s", affKVP.Key)] = func(ctx context.Context, object *model.KVPair) (*model.KVPair, error) {
				// Create the affinity for the other racing host.
				_, err := bc.Create(ctx, affKVP2)
				if err != nil {
					return nil, err
				}

				// And create it for the host requesting it.
				return bc.Create(ctx, object)
			}

			// Creation function for the actual block - should return an error indicating the block
			// was already taken by another host.
			aff := fmt.Sprintf("host:%s", host)
			aff2 := "host:another-host"
			b := newBlock(*net)
			b.Affinity = &aff
			b.StrictAffinity = false
			blockKVP := &model.KVPair{
				Key:   model.BlockKey{*net},
				Value: b.AllocationBlock,
			}

			b2 := newBlock(*net)
			b2.Affinity = &aff2
			b2.StrictAffinity = false
			blockKVP2 := &model.KVPair{
				Key:   model.BlockKey{*net},
				Value: b2.AllocationBlock,
			}
			fc.createFuncs[fmt.Sprintf("%s", blockKVP.Key)] = func(ctx context.Context, object *model.KVPair) (*model.KVPair, error) {
				// Create the "stolen" affinity from the other racing host.
				_, err := bc.Create(ctx, blockKVP2)
				if err != nil {
					return nil, err
				}

				// Return that the object already exists.
				return nil, cerrors.ErrorResourceAlreadyExists{}
			}

			// Get function for the block. The first time, it should return nil to indicate nobody has the block. On subsequent calls,
			// return the real data from etcd representing the block belonging to another host.
			calls := 0
			fc.getFuncs[fmt.Sprintf("%s", blockKVP.Key)] = func(ctx context.Context, k model.Key, r string) (*model.KVPair, error) {
				return func(ctx context.Context, k model.Key, r string) (*model.KVPair, error) {
					calls = calls + 1

					if calls == 1 {
						// First time the block doesn't exist yet.
						return nil, cerrors.ErrorResourceDoesNotExist{}
					}
					return bc.Get(ctx, k, r)
				}(ctx, k, r)
			}

			fc.getFuncs["default"] = func(ctx context.Context, k model.Key, r string) (*model.KVPair, error) {
				return bc.Get(ctx, k, r)
			}

			// Delete function for the affinity - this should fail, triggering the scenario under test where two hosts now think they
			// have affinity to the block.
			deleteCalls := 0
			fc.deleteFuncs[fmt.Sprintf("%s", affKVP.Key)] = func(ctx context.Context, k model.Key, r string) (*model.KVPair, error) {
				return func(ctx context.Context, k model.Key, r string) (*model.KVPair, error) {
					deleteCalls = deleteCalls + 1

					if deleteCalls == 1 {
						// First time around, the delete fails - this triggers the scenario.
						return nil, fmt.Errorf("block affinity deletion failure")
					}

					// Subsequent calls succeed.
					return bc.Delete(ctx, k, r)
				}(ctx, k, r)
			}

			// List function should behave normally.
			fc.listFuncs["default"] = func(ctx context.Context, list model.ListInterface, revision string) (*model.KVPairList, error) {
				return bc.List(ctx, list, revision)
			}

			// Create the block reader / writer which will simulate the failure scenario.
			pools := &ipPoolAccessor{pools: map[string]bool{}}
			pools.pools["10.0.0.0/26"] = true
			rw := blockReaderWriter{client: fc, pools: pools}
			ipamClient := &ipamClient{
				client:            bc,
				pools:             pools,
				blockReaderWriter: rw,
			}

			By("attempting to claim the block on multiple hosts at the same time", func() {
				ips, err := ipamClient.autoAssign(ctx, 1, nil, nil, nil, ipv4, host)

				// Shouldn't return an error.
				Expect(err).NotTo(HaveOccurred())

				// Should return a single IP.
				Expect(len(ips)).To(Equal(1))
			})

			By("checking that the other host has the affinity", func() {
				// The block should have the affinity field set properly.
				opts := model.BlockAffinityListOptions{Host: otherHost}
				objs, err := rw.client.List(ctx, opts, "")
				Expect(err).NotTo(HaveOccurred())

				// Should be a single block affinity, assigned to the other host.
				Expect(len(objs.KVPairs)).To(Equal(1))
				Expect(objs.KVPairs[0].Value.(*model.BlockAffinity).Pending).NotTo(BeTrue())
			})

			By("checking that the test host has a pending affinity", func() {
				// The block should have the affinity field set properly.
				opts := model.BlockAffinityListOptions{Host: host}
				objs, err := rw.client.List(ctx, opts, "")
				Expect(err).NotTo(HaveOccurred())
				Expect(len(objs.KVPairs)).To(Equal(1))
				Expect(objs.KVPairs[0].Value.(*model.BlockAffinity).Pending).To(BeTrue())
			})

			By("attempting to claim another address", func() {
				ips, err := ipamClient.autoAssign(ctx, 1, nil, nil, nil, ipv4, host)

				// Shouldn't return an error.
				Expect(err).NotTo(HaveOccurred())

				// Should return a single IP.
				Expect(len(ips)).To(Equal(1))
			})

			By("checking that the pending affinity was cleaned up", func() {
				// The block should have the affinity field set properly.
				opts := model.BlockAffinityListOptions{Host: host}
				objs, err := rw.client.List(ctx, opts, "")
				Expect(err).NotTo(HaveOccurred())

				// Should be a single block affinity, assigned to the other host.
				Expect(len(objs.KVPairs)).To(Equal(0))
			})
		})
	})

	Context("IPAM block allocation race conditions", func() {
		It("should clean up pending block allocations", func() {
			var bc api.Client
			By("creating a backend client", func() {
				var err error
				bc, err = backend.NewClient(config)
				Expect(err).NotTo(HaveOccurred())
			})

			var net *cnet.IPNet
			By("picking a block cidr", func() {
				var err error
				_, net, err = cnet.ParseCIDR("10.1.0.0/26")
				Expect(err).NotTo(HaveOccurred())
			})

			By("claiming affinity for a block on another host", func() {
				kvp := &model.KVPair{
					Key:   model.BlockAffinityKey{CIDR: *net, Host: "host-1"},
					Value: &model.BlockAffinity{Pending: false},
				}
				_, err := bc.Create(ctx, kvp)
				Expect(err).NotTo(HaveOccurred())
			})

			By("claiming block on another host", func() {
				aff := "host-1"
				kvp := &model.KVPair{
					Key: model.BlockKey{CIDR: *net},
					Value: &model.AllocationBlock{
						CIDR:     *net,
						Affinity: &aff,
					},
				}
				_, err := bc.Create(ctx, kvp)
				Expect(err).NotTo(HaveOccurred())
			})

			By("claiming a pending affinity for a block on this host", func() {
				kvp := &model.KVPair{
					Key:   model.BlockAffinityKey{CIDR: *net, Host: "host-2"},
					Value: &model.BlockAffinity{Pending: true},
				}
				_, err := bc.Create(ctx, kvp)
				Expect(err).NotTo(HaveOccurred())
			})

			p := &ipPoolAccessor{pools: map[string]bool{}}
			ic := &ipamClient{
				client: bc,
				pools:  p,
				blockReaderWriter: blockReaderWriter{
					client: bc,
					pools:  p,
				},
			}
			By("verifying the affinity of the first host", func() {
				valid := ic.verifyAffinity(ctx, "host-1", *net)
				Expect(valid).To(BeTrue())
			})

			By("verifying the affinity of the second host", func() {
				valid := ic.verifyAffinity(ctx, "host-2", *net)
				Expect(valid).NotTo(BeTrue())
			})

			By("confirming the affinity for the second host has been removed", func() {
				k := model.BlockAffinityKey{CIDR: *net, Host: "host-2"}
				_, err := bc.Get(ctx, k, "")
				Expect(err).To(HaveOccurred())
			})
		})
	})

})
