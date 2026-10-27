package chaos

import (
	"fmt"

	"github.com/anishathalye/porcupine"
	"github.com/zeiado/kvraft/kv"
)

// opInput and opOutput are what the checker sees for each client operation.
type opInput struct {
	Type, Key, Value, Expected string
}

type opOutput struct {
	Value string
	Found bool
	OK    bool
}

type kvState struct {
	value  string
	exists bool
}

// Model is the sequential spec of a single key. Histories are partitioned by
// key, which keeps checking fast.
var Model = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var keys []string
		for _, op := range history {
			k := op.Input.(opInput).Key
			if _, ok := byKey[k]; !ok {
				keys = append(keys, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(keys))
		for _, k := range keys {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return kvState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st, in, out := state.(kvState), input.(opInput), output.(opOutput)
		switch in.Type {
		case kv.OpGet:
			return out.Found == st.exists && (!st.exists || out.Value == st.value), st
		case kv.OpPut:
			return true, kvState{value: in.Value, exists: true}
		case kv.OpDelete:
			return out.Found == st.exists, kvState{}
		case kv.OpCAS:
			if st.exists && st.value == in.Expected {
				return out.OK, kvState{value: in.Value, exists: true}
			}
			return !out.OK, st
		}
		return false, st
	},
	DescribeOperation: func(input, output interface{}) string {
		in, out := input.(opInput), output.(opOutput)
		switch in.Type {
		case kv.OpGet:
			if !out.Found {
				return fmt.Sprintf("get(%s) -> <none>", in.Key)
			}
			return fmt.Sprintf("get(%s) -> %q", in.Key, out.Value)
		case kv.OpPut:
			return fmt.Sprintf("put(%s, %q)", in.Key, in.Value)
		case kv.OpDelete:
			return fmt.Sprintf("delete(%s) -> found=%v", in.Key, out.Found)
		case kv.OpCAS:
			return fmt.Sprintf("cas(%s, %q -> %q) -> %v", in.Key, in.Expected, in.Value, out.OK)
		}
		return "?"
	},
	DescribeState: func(state interface{}) string {
		st := state.(kvState)
		if !st.exists {
			return "<none>"
		}
		return fmt.Sprintf("%q", st.value)
	},
}
