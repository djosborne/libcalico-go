// Copyright (c) 2017 Tigera, Inc. All rights reserved.

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

package conversionv1v3

import (
	"testing"

	. "github.com/onsi/gomega"

	apiv1 "github.com/projectcalico/libcalico-go/lib/apis/v1"
	"github.com/projectcalico/libcalico-go/lib/apis/v1/unversioned"
	apiv3 "github.com/projectcalico/libcalico-go/lib/apis/v3"
	"github.com/projectcalico/libcalico-go/lib/backend/model"
	cnet "github.com/projectcalico/libcalico-go/lib/net"
	"github.com/projectcalico/libcalico-go/lib/numorstring"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
)

var asn, _ = numorstring.ASNumberFromString("1")
var netv4 = cnet.MustParseNetwork("192.168.1.1/32")
var netv6 = cnet.MustParseCIDR("fed::/64")
var ipv4 = cnet.MustParseIP("192.168.1.1")
var ipv6 = cnet.MustParseIP("fed::")

var hepTable = []struct {
	description string
	v1API       unversioned.Resource
	v1KVP       *model.KVPair
	v3API       apiv3.Node
}{
	{
		description: "Valid basic v1 hep has data moved to right place",
		v1API: apiv1.Node{
			Metadata: apiv1.NodeMetadata{
				Name: "my-node",
			},
			Spec: apiv1.NodeSpec{
				BGP: &apiv1.NodeBGPSpec{
					ASNumber:    &asn,
					IPv4Address: &netv4,
					IPv6Address: &netv6,
				},
			},
		},
		v1KVP: &model.KVPair{
			Key: model.NodeKey{
				Hostname: "my-node",
			},
			Value: &model.Node{
				BGPASNumber: &asn,
				BGPIPv4Addr: &ipv4,
				BGPIPv6Addr: &ipv6,
			},
		},
		v3API: apiv3.Node{
			ObjectMeta: v1.ObjectMeta{
				Name: "my-node",
			},
			Spec: apiv3.NodeSpec{
				BGP: &apiv3.NodeBGPSpec{
					ASNumber:    &asn,
					IPv4Address: "192.168.1.1",
					IPv6Address: "fed::",
				},
			},
		},
	},
}

func TestCanConvertV1ToV3Node(t *testing.T) {
	for _, tdata := range hepTable {
		t.Run(tdata.description, func(t *testing.T) {
			RegisterTestingT(t)

			p := Node{}
			// Check v1API->v1KVP.
			convertedKvp, err := p.APIV1ToBackendV1(tdata.v1API)
			Expect(err).NotTo(HaveOccurred(), tdata.description)

			Expect(convertedKvp.Key.(model.NodeKey)).To(Equal(tdata.v1KVP.Key.(model.NodeKey)))
			Expect(convertedKvp.Value.(*model.Node)).To(Equal(tdata.v1KVP.Value))

			// Check v1KVP->v3API.
			convertedv3, err := p.BackendV1ToAPIV3(tdata.v1KVP)
			Expect(err).NotTo(HaveOccurred(), tdata.description)
			Expect(convertedv3.(*apiv3.Node).ObjectMeta).To(Equal(tdata.v3API.ObjectMeta), tdata.description)
			Expect(convertedv3.(*apiv3.Node).Spec).To(Equal(tdata.v3API.Spec), tdata.description)
		})
	}
}
