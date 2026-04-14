package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/output"
)

func newAuthInspectTokenCmd(ctx *cmdContext) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "inspect-token",
		Short: "Decode and display the stored authentication tokens",
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAuthInspectToken(cmd, ctx)
		},
	}
	return cmd
}

type tokenInspection struct {
	TokenType   string           `json:"token_type"`
	Expiry      time.Time        `json:"expiry"`
	Status      string           `json:"status"`
	HasRefresh  bool             `json:"has_refresh_token"`
	AccessToken *auth.DecodedJWT `json:"access_token,omitempty"`
	IDToken     *auth.DecodedJWT `json:"id_token,omitempty"`
}

func runAuthInspectToken(cmd *cobra.Command, ctx *cmdContext) error {
	store := auth.KeyringTokenStore{}
	tokens, err := store.Load(cmd.Context())
	if err != nil {
		return fmt.Errorf("load tokens (run 'fleetctl auth login' first): %w", err)
	}

	result := tokenInspection{
		TokenType:  tokens.TokenType,
		Expiry:     tokens.Expiry,
		HasRefresh: tokens.RefreshToken != "",
	}

	remaining := time.Until(tokens.Expiry)
	if remaining > 0 {
		result.Status = fmt.Sprintf("Valid (expires in %s)", formatDuration(remaining))
	} else {
		result.Status = fmt.Sprintf("Expired (%s ago)", formatDuration(-remaining))
	}

	if tokens.AccessToken != "" {
		if d, err := auth.DecodeJWT(tokens.AccessToken); err == nil {
			result.AccessToken = &d
		}
	}
	if tokens.IDToken != "" {
		if d, err := auth.DecodeJWT(tokens.IDToken); err == nil {
			result.IDToken = &d
		}
	}

	if output.Format(ctx.flags.outputFormat) == output.FormatJSON {
		enc := json.NewEncoder(cmd.OutOrStdout())
		enc.SetIndent("", "  ")
		return enc.Encode(result)
	}

	w := cmd.OutOrStdout()
	fmt.Fprintf(w, "Token Type:     %s\n", result.TokenType)
	fmt.Fprintf(w, "Expiry:         %s\n", tokens.Expiry.Format(time.RFC3339))
	fmt.Fprintf(w, "Status:         %s\n", result.Status)
	fmt.Fprintf(w, "Refresh Token:  %t\n", result.HasRefresh)

	if result.AccessToken != nil {
		fmt.Fprintf(w, "\nAccess Token Claims:\n")
		printClaims(w, result.AccessToken.Claims)
	}
	if result.IDToken != nil {
		fmt.Fprintf(w, "\nID Token Claims:\n")
		printClaims(w, result.IDToken.Claims)
	}

	return nil
}

var knownClaimOrder = []string{"sub", "iss", "aud", "exp", "iat", "nbf", "email", "groups", "azp"}

func printClaims(w io.Writer, claims map[string]any) {
	printed := make(map[string]bool, len(knownClaimOrder))
	for _, k := range knownClaimOrder {
		v, ok := claims[k]
		if !ok {
			continue
		}
		printed[k] = true
		fmt.Fprintf(w, "  %-12s %s\n", k+":", formatClaimValue(k, v))
	}

	extras := make([]string, 0, len(claims))
	for k := range claims {
		if !printed[k] {
			extras = append(extras, k)
		}
	}
	sort.Strings(extras)
	for _, k := range extras {
		fmt.Fprintf(w, "  %-12s %s\n", k+":", formatClaimValue(k, claims[k]))
	}
}

func formatClaimValue(key string, v any) string {
	switch key {
	case "exp", "iat", "nbf":
		if n, ok := v.(float64); ok {
			sec, frac := math.Modf(n)
			t := time.Unix(int64(sec), int64(frac*1e9)).UTC()
			return t.Format(time.RFC3339)
		}
	}

	switch val := v.(type) {
	case []any:
		parts := make([]string, len(val))
		for i, item := range val {
			parts[i] = fmt.Sprint(item)
		}
		return strings.Join(parts, ", ")
	default:
		return fmt.Sprint(val)
	}
}

func formatDuration(d time.Duration) string {
	d = d.Round(time.Second)
	h := int(d.Hours())
	m := int(d.Minutes()) % 60
	s := int(d.Seconds()) % 60

	switch {
	case h > 0:
		return fmt.Sprintf("%dh%dm", h, m)
	case m > 0:
		return fmt.Sprintf("%dm%ds", m, s)
	default:
		return fmt.Sprintf("%ds", s)
	}
}
