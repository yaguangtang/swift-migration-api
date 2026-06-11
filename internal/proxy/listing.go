package proxy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

func listingFormat(query url.Values) string {
	format := strings.ToLower(query.Get("format"))
	if format == "" {
		return "plain"
	}
	return format
}

func mergeListingBodies(query url.Values, cephBody, swiftBody []byte) ([]byte, string, error) {
	switch listingFormat(query) {
	case "plain":
		return mergePlainListing(query, cephBody, swiftBody), "text/plain; charset=utf-8", nil
	case "json":
		payload, err := mergeJSONListing(query, cephBody, swiftBody)
		if err != nil {
			return nil, "", err
		}
		return payload, "application/json; charset=utf-8", nil
	default:
		return nil, "", fmt.Errorf("unsupported listing format %q", query.Get("format"))
	}
}

func mergePlainListing(query url.Values, cephBody, swiftBody []byte) []byte {
	items := make(map[string]struct{})
	for _, body := range [][]byte{cephBody, swiftBody} {
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" {
				continue
			}
			items[trimmed] = struct{}{}
		}
	}

	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	keys = applyLimit(query, keys)

	if len(keys) == 0 {
		return nil
	}
	return []byte(strings.Join(keys, "\n") + "\n")
}

func mergeJSONListing(query url.Values, cephBody, swiftBody []byte) ([]byte, error) {
	merged := make(map[string]map[string]any)
	for _, body := range [][]byte{cephBody, swiftBody} {
		var items []map[string]any
		if len(bytes.TrimSpace(body)) == 0 {
			continue
		}
		if err := json.Unmarshal(body, &items); err != nil {
			return nil, fmt.Errorf("decode listing json: %w", err)
		}
		for _, item := range items {
			key := listingEntryKey(item)
			if key == "" {
				continue
			}
			if _, exists := merged[key]; exists {
				continue
			}
			merged[key] = item
		}
	}

	keys := make([]string, 0, len(merged))
	for key := range merged {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	keys = applyLimit(query, keys)

	out := make([]map[string]any, 0, len(keys))
	for _, key := range keys {
		out = append(out, merged[key])
	}
	return json.Marshal(out)
}

func listingEntryKey(item map[string]any) string {
	if name, ok := item["name"].(string); ok && name != "" {
		return name
	}
	if subdir, ok := item["subdir"].(string); ok && subdir != "" {
		return subdir
	}
	return ""
}

func applyLimit(query url.Values, keys []string) []string {
	raw := query.Get("limit")
	if raw == "" {
		return keys
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit < 0 || limit >= len(keys) {
		return keys
	}
	return keys[:limit]
}
