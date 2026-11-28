package bwtree

import (
	"github.com/swytchdb/engine/graph"
	"github.com/swytchdb/engine/graph/bwtree/meta"
)

type Tree struct {
	*graph.Tree
	meta *meta.TreeMeta
}
