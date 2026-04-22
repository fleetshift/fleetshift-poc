package application

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/google/cel-go/cel"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// EvalCELClaim evaluates a CEL expression against the raw claims of a
// JWT. The expression receives a single variable "claims" of type
// map(string, dyn) and must produce a string result.
func EvalCELClaim(expression string, rawToken string) (string, error) {
	claims, err := extractAllClaims(rawToken)
	if err != nil {
		return "", fmt.Errorf("extract claims from ID token: %w", err)
	}

	env, err := cel.NewEnv(
		cel.Variable("claims", cel.DynType),
	)
	if err != nil {
		return "", fmt.Errorf("create CEL environment: %w", err)
	}

	ast, issues := env.Compile(expression)
	if issues != nil && issues.Err() != nil {
		return "", fmt.Errorf("compile CEL expression %q: %w", expression, issues.Err())
	}

	prg, err := env.Program(ast)
	if err != nil {
		return "", fmt.Errorf("program CEL: %w", err)
	}

	out, _, err := prg.Eval(map[string]any{"claims": claims})
	if err != nil {
		return "", fmt.Errorf("evaluate CEL expression: %w", err)
	}

	s, ok := out.Value().(string)
	if !ok {
		return "", fmt.Errorf("CEL expression must produce a string, got %T", out.Value())
	}
	if s == "" {
		return "", fmt.Errorf("CEL expression produced an empty string")
	}
	return s, nil
}

// EvalClaimMapping evaluates a [domain.RegistrySubjectMapping]'s CEL
// expression against the raw claims of a JWT.
func EvalClaimMapping(mapping *domain.RegistrySubjectMapping, rawToken string) (domain.RegistrySubject, error) {
	if mapping == nil {
		return "", fmt.Errorf("no registry subject mapping configured")
	}
	s, err := EvalCELClaim(mapping.Expression, rawToken)
	if err != nil {
		return "", err
	}
	return domain.RegistrySubject(s), nil
}

// extractAllClaims parses the JWT payload without signature
// verification and returns the claims as a map. This is safe because
// the token has already been verified by the OIDC verifier before
// this is called.
func extractAllClaims(rawToken string) (map[string]any, error) {
	parts := splitJWT(rawToken)
	if len(parts) != 3 {
		return nil, fmt.Errorf("malformed JWT: expected 3 parts")
	}
	payload, err := jwtBase64Decode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("decode JWT payload: %w", err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, fmt.Errorf("unmarshal JWT payload: %w", err)
	}
	return claims, nil
}

func splitJWT(token string) []string {
	var parts []string
	start := 0
	for i := range token {
		if token[i] == '.' {
			parts = append(parts, token[start:i])
			start = i + 1
		}
	}
	parts = append(parts, token[start:])
	return parts
}

func jwtBase64Decode(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(s)
}
