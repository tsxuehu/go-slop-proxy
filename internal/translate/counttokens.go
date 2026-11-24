package translate

import "encoding/json"

// Estimate approximates input token count: chars/4 + per-item overhead + images + 30.
func Estimate(req *ResponsesRequest) int64 {
	chars := len(req.Instructions)
	var overhead, imageTokens int64
	for _, item := range req.Input {
		overhead += 4
		switch item.Type {
		case "message":
			for _, p := range item.Content {
				switch p.Type {
				case "input_text", "output_text":
					chars += len(p.Text)
				case "input_image":
					imageTokens += 1000
				}
			}
		case "function_call":
			chars += len(item.Name) + len(item.Arguments)
		case "function_call_output", "custom_tool_call_output":
			if item.Output != nil {
				chars += len(item.Output.TextValue())
			}
		case "custom_tool_call":
			chars += len(item.Name) + len(item.Input)
		case "additional_tools":
			for _, t := range item.Tools {
				b, _ := json.Marshal(t)
				chars += len(b)
			}
		case "reasoning":
			for _, s := range item.Summary {
				chars += len(s.Text)
			}
		}
	}
	for _, t := range req.Tools {
		b, _ := json.Marshal(t)
		chars += len(b)
	}
	return int64(chars/4) + overhead + imageTokens + 30
}
