package bplus

import "encoding/json"

// Addresses are binary strings. JSON text strings would replace invalid UTF-8,
// so references use the same byte encoding as entry keys and values on disk.
type pageRecord struct {
	Leaf     bool     `json:"leaf"`
	Parent   []byte   `json:"parent,omitempty"`
	Keys     [][]byte `json:"keys,omitempty"`
	Values   [][]byte `json:"values,omitempty"`
	Children [][]byte `json:"children,omitempty"`
	Prev     []byte   `json:"prev,omitempty"`
	Next     []byte   `json:"next,omitempty"`
	Splits   uint64   `json:"splits,omitempty"`
}

func encodePage(p *page) ([]byte, error) {
	r := pageRecord{
		Leaf: p.Leaf, Parent: []byte(p.Parent), Keys: p.Keys, Values: p.Values,
		Prev: []byte(p.Prev), Next: []byte(p.Next), Splits: p.Splits,
	}
	for _, child := range p.Children {
		r.Children = append(r.Children, []byte(child))
	}

	return json.Marshal(r)
}

func decodePage(raw []byte, p *page) error {
	var r pageRecord
	if err := json.Unmarshal(raw, &r); err != nil {
		return err
	}

	*p = page{
		Leaf: r.Leaf, Parent: string(r.Parent), Keys: r.Keys, Values: r.Values,
		Prev: string(r.Prev), Next: string(r.Next), Splits: r.Splits,
	}
	for _, child := range r.Children {
		p.Children = append(p.Children, string(child))
	}

	return nil
}
