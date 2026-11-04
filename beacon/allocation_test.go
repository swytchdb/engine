package beacon

import (
	"fmt"
	"testing"

	pb "github.com/swytchdb/engine/cluster/proto"
	"github.com/swytchdb/engine/effects"
)

// Measure the whole unchanged-projection path, including snapshot reads.
func BenchmarkMembershipRefresh(b *testing.B) {
	for _, count := range []int{1, 32, 256} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			e := effects.NewEngine(effects.EngineConfig{})

			b.Cleanup(func() {
				if err := e.Close(); err != nil {
					b.Error(err)
				}
			})

			ctx := e.NewContext()
			for id := range count {
				if err := ctx.Emit(buildMemberInsert(uint64(id+1), "127.0.0.1:7380")); err != nil {
					b.Fatal(err)
				}
			}

			if err := ctx.Flush(); err != nil {
				b.Fatal(err)
			}

			snapshot, _, err := e.NewReadOnlyContext().GetSnapshot(MembershipKey)
			if err != nil {
				b.Fatal(err)
			}

			beacon := New(Config{NodeID: e.NodeID()}, e, nil)
			beacon.members = parseMembership(snapshot)

			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				if err := beacon.refreshTopology(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkParseMembership(b *testing.B) {
	snapshot := &pb.ReducedEffect{NetAdds: make(map[string]*pb.ReducedElement, 256)}

	for id := range 256 {
		data := buildMemberInsert(uint64(id+1), "127.0.0.1:7380").GetData()
		snapshot.NetAdds[string(data.GetId())] = &pb.ReducedElement{Data: data}
	}

	b.ReportAllocs()

	for b.Loop() {
		parseMembership(snapshot)
	}
}

func BenchmarkParseLiteralAddrs(b *testing.B) {
	for _, input := range []string{"127.0.0.1:7380", "127.0.0.1,127.0.0.2,127.0.0.3"} {
		b.Run(input, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				parseLiteralAddrs(input, 7380)
			}
		})
	}
}
