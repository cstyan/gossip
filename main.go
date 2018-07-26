package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	listenAddress  = kingpin.Flag("listen.address", "Address to listen on for gossip protocol.").Short('l').Required().String()
	gossipInterval = kingpin.Flag("gossip.interval", "Timeout interval for gossip heartbeats in seconds.").Short('g').Required().Int()
	initialPeer    = kingpin.Flag("initial.peer", "Address of initial peer to gossip with when joining cluster.").Short('i').String()

	gMembers *members
)

type members struct {
	self                 string
	members              map[string]struct{}
	randomizedMemberList []string
	lock                 sync.RWMutex
}

func (m *members) initialMembers() {
	m.lock.Lock()
	m.members[m.self] = struct{}{}
	if *initialPeer != "" {
		m.members[*initialPeer] = struct{}{}
	}
	m.lock.Unlock()
	m.randomizedMemberList[0] = *initialPeer
}

func (m *members) randomizeMembers() {
	m.lock.RLock()
	len := len(m.randomizedMemberList)
	m.randomizedMemberList = make([]string, 0, len)
	for k := range m.members {
		// We don't want to gossip with ourselves.
		if k == *listenAddress {
			continue
		}
		fmt.Println("appending to list in randomize: ", k)
		m.randomizedMemberList = append(m.randomizedMemberList, k)
	}
	m.lock.RUnlock()
}

// Take the first member off the list and put it at the end, return that member.
func (m *members) getRandomMember() (string, error) {
	fmt.Println("list: ", m.randomizedMemberList)
	rand := m.randomizedMemberList[0]
	fmt.Println("rand: ", rand)
	if rand == "" {
		return rand, fmt.Errorf("no members in cluster to gossip with")
	}
	m.randomizedMemberList = m.randomizedMemberList[1:]
	fmt.Println("appending to list in get random member: ", rand)
	fmt.Println("list: ", m.randomizedMemberList)
	fmt.Println("len: ", len(m.randomizedMemberList))
	m.randomizedMemberList = append(m.randomizedMemberList, rand)
	return rand, nil
}

func newMembers(self string) *members {
	return &members{
		self:                 self,
		members:              make(map[string]struct{}),
		randomizedMemberList: make([]string, 1),
	}
}

// Handle incomming gossip heartbeats from peers.
func gossipHandler(writer http.ResponseWriter, req *http.Request) {
	// Parse the json which should have the peers list of cluster members.
	fmt.Println("got a request to gossip handler")
	members := make(map[string]struct{})
	defer req.Body.Close()
	b, _ := ioutil.ReadAll(req.Body)
	json.Unmarshal(b, &members)
	gMembers.lock.Lock()
	for k := range members {
		fmt.Println("adding to members: ", k)
		gMembers.members[k] = struct{}{}
	}
	fmt.Printf("members: %+v\n", gMembers.members)
	gMembers.lock.Unlock()
	go gMembers.randomizeMembers()
	// We should respond with our member list here.
	writer.Write([]byte("success"))
}

func main() {
	kingpin.Parse()

	// Set our intial set of known cluster members, just ourselves.
	gMembers = newMembers(*listenAddress)
	gMembers.initialMembers()

	// Ticker loop with first tick happening "immediately".
	go func() {
		fmt.Println("starting go routine")
		for c := time.Tick(time.Duration(*gossipInterval) * time.Second); ; {
			// TODO: Attempt to get the first item from the randomized members list.
			// Don't just assume it exists.
			peer, err := gMembers.getRandomMember()
			membersPayload, err := json.Marshal(gMembers.members)
			if err != nil {
				fmt.Println("error getting random member: ", err)
				goto WAIT
			}
			fmt.Println("trying to gossip to peer: ", peer)
			_, err = http.Post(fmt.Sprintf("http://%s/gossip", peer), "json", bytes.NewBuffer(membersPayload))
			if err != nil {
				fmt.Println("err: ", err)
			}

			// Wait for ticker.
		WAIT:
			select {
			case <-c:
				fmt.Println("select")
				continue
			}
		}
	}()

	// Setup HTTP endpoint for incomming gossip messages from peers.
	r := mux.NewRouter()
	r.HandleFunc("/gossip", gossipHandler).Methods("POST")
	http.Handle("/", r)
	fmt.Println("starting http server")
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		fmt.Println("err: ", err)
	}
}
