package main

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestQuotaInvalidUser(t *testing.T) {
	m := NewManager()
	m.Run()

	err := m.UseToken("user1")
	if err == nil {
		t.Fatalf("Should have returned an error for an invalid user")
	}
	if err != ErrRuleDoesNotExist {
		t.Fatalf("Should not have allowed a token use for user1")
	}
}

func TestQuotaValidUser(t *testing.T) {
	m := NewManager()
	m.Run()

	user := "user1"
	m.AddRule(user, NewRule(1, 5*time.Second))

	err := m.UseToken(user)
	if err != nil {
		t.Fatalf("Did not expect an error on valid user, %v", err)
	}
}

func TestQuotaCountMax(t *testing.T) {
	m := NewManager()
	m.Run()

	m.AddRule("user1", NewRule(1, 5*time.Second))
	m.AddRule("user2", NewRule(1, 1*time.Second))
	m.AddRule("user3", NewRule(4, 2*time.Second))

	m.Lock()
	for k, r := range m.rules {
		if r.count != r.maxQueries {
			t.Fatalf("Expected %d tokens available but got %d, for %d", r.maxQueries, r.count, k)
		}
	}
	m.Unlock()
}

func BenchmarkQuotaUpdateMillionKeys(b *testing.B) {
	m := NewManager()

	for i := 0; i < 1000000; i++ {
		m.AddRule(strconv.Itoa(i), NewRule(1, 30*time.Second))
	}
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		m.addTokens()
	}
}

func BenchmarkQuotaUseMillionKeys(b *testing.B) {
	m := NewManager()
	m.Run()

	numKeys := 1000000
	for i := 0; i < numKeys; i++ {
		m.AddRule(strconv.Itoa(i), NewRule(1, 5*time.Second))
	}

	b.ResetTimer()
	var numOk int
	for n := 0; n < b.N; n++ {
		groups := 256
		var wg sync.WaitGroup
		wg.Add(groups)
		for i := 0; i < groups; i++ {
			go func(group int) {
				for j := 0; j < numKeys/groups; j++ {
					if err := m.UseToken(strconv.Itoa((j*groups + group) % numKeys)); err == nil {
						numOk++
					}
				}
				wg.Done()
			}(i)
		}
		wg.Wait()
	}
	b.Logf("Got %d ok out of %d", numOk, b.N)
}
