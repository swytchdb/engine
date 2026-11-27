package effects

import pb "github.com/swytchdb/engine/cluster/proto"

// ancestrySearch answers multiple reachability questions from the same roots
// with one lazy, key-scoped traversal. Missing vertices terminate a branch;
// only real dependency edges establish reachability. Keep it local to a check
// so a later check can retry history that was unavailable during this one.
type ancestrySearch struct {
	key   string
	load  func(Tip) (*pb.Effect, error)
	seen  map[Tip]bool
	stack []Tip
}

func newAncestrySearch(key string, roots []Tip, load func(Tip) (*pb.Effect, error)) *ancestrySearch {
	s := &ancestrySearch{key: key, load: load, seen: make(map[Tip]bool)}
	for _, tip := range roots {
		s.push(tip)
	}

	return s
}

func (s *ancestrySearch) push(tip Tip) {
	if tip == (Tip{}) || s.seen[tip] {
		return
	}

	s.seen[tip] = true
	s.stack = append(s.stack, tip)
}

func (s *ancestrySearch) contains(target Tip) bool {
	if target == (Tip{}) {
		return false
	}

	if s.seen[target] {
		return true
	}

	for len(s.stack) != 0 {
		tip := s.stack[len(s.stack)-1]
		s.stack = s.stack[:len(s.stack)-1]

		eff, err := s.load(tip)
		if err != nil || (eff.GetTxnBind() == nil && string(eff.GetIKey()) != s.key) {
			continue
		}

		for ref := range keyDeps(s.key, eff) {
			s.push(r(ref))
		}

		if s.seen[target] {
			return true
		}
	}

	return false
}
