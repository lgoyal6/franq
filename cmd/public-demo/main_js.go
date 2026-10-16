//go:build js && wasm

package main

import (
	"encoding/json"
	"fmt"
	"github.com/lgoyal6/franq/internal/core"
	"syscall/js"
)

type input struct {
	Jobs  []core.BrowserMessage `json:"jobs"`
	Order string                `json:"order"`
	Aging float64               `json:"aging"`
	NowMS int64                 `json:"now_ms"`
}

func evaluate(raw []byte) (any, error) {
	var in input
	if err := json.Unmarshal(raw, &in); err != nil {
		return nil, err
	}
	order, waiting, err := core.BrowserOrder(in.Jobs, in.Order, in.Aging, in.NowMS)
	if err != nil {
		return nil, err
	}
	return map[string]any{"order": order, "waiting": waiting}, nil
}

func main() {
	callback := js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) != 1 || args[0].Type() != js.TypeString {
			return `{"error":"Expected one JSON input"}`
		}
		raw := args[0].String()
		var result any
		if len(raw) > 32768 {
			result = map[string]string{"error": "Input exceeds 32 KiB"}
		} else {
			value, err := evaluate([]byte(raw))
			if err != nil {
				result = map[string]string{"error": err.Error()}
			} else {
				result = value
			}
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return fmt.Sprintf(`{"error":%q}`, err.Error())
		}
		return string(encoded)
	})
	js.Global().Set("publicDemo", callback)
	select {}
}
