package steps

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

const (
	// Query resourceType values are the API type identity ({service}/{Type}),
	// not the fleetctl collection spelling (kind.fleetshift.v1/clusters).
	kindClusterQueryType      = "kind.fleetshift.io/Cluster"
	kindNodeQueryType         = "kind.fleetshift.io/Node"
	kubernetesObjectQueryType = "kubernetes.fleetshift.io/Object"

	queryCommandTimeout  = 10 * time.Second
	queryPollInterval    = clusterPollInterval
	kindIndexWaitTimeout = 1 * time.Minute

	queryInspectPageSize    int32 = 50
	queryPaginationPageSize int32 = 10
	queryPaginationOrderBy        = "resource_type,name"
	queryPaginationMaxPages       = 100

	kindNodeEnvelopePrefix = "//kind.fleetshift.io/nodes/"
)

// queryPage is fleetctl resource query JSON (--output json).
type queryPage struct {
	Resources     []queryHit `json:"resources"`
	NextPageToken string     `json:"nextPageToken"`
}

// queryHit is one resource query result: envelope identity plus the Get/List body.
type queryHit struct {
	Name         string          `json:"name"`
	ResourceType string          `json:"resourceType"`
	Resource     json.RawMessage `json:"resource"`
}

// queryObservation is the inventory observation object on a query body.
type queryObservation struct {
	Kind      string         `json:"kind"`
	Cluster   string         `json:"cluster"`
	GVR       queryGVR       `json:"gvr"`
	Metadata  queryMetadata  `json:"metadata"`
	Extracted map[string]any `json:"extracted"`
}

// queryGVR is the group/version/resource identity on a kubernetes Object observation.
type queryGVR struct {
	Group    string `json:"group"`
	Version  string `json:"version"`
	Resource string `json:"resource"`
	Scope    string `json:"scope"`
}

// queryMetadata is object metadata on a kubernetes Object observation.
type queryMetadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
}

// resourceQueryRequest is one fleetctl resource query invocation.
type resourceQueryRequest struct {
	filter    string
	pageSize  int32
	pageToken string
	orderBy   string
}

// args returns fleetctl resource query flags for this request. Empty fields are omitted.
func (r resourceQueryRequest) args() []string {
	args := []string{"resource", "query"}
	if r.filter != "" {
		args = append(args, "--filter", r.filter)
	}
	if r.pageSize > 0 {
		args = append(args, "--page-size", strconv.FormatInt(int64(r.pageSize), 10))
	}
	if r.pageToken != "" {
		args = append(args, "--page-token", r.pageToken)
	}
	if r.orderBy != "" {
		args = append(args, "--order-by", r.orderBy)
	}
	return args
}

// inspectQuery is a single-page resource query with the inspect page size.
func inspectQuery(filter string) resourceQueryRequest {
	return resourceQueryRequest{filter: filter, pageSize: queryInspectPageSize}
}

// parseQueryPage unmarshals fleetctl resource query JSON.
func parseQueryPage(stdout string) (queryPage, error) {
	var page queryPage
	if err := json.Unmarshal([]byte(stdout), &page); err != nil {
		return queryPage{}, fmt.Errorf("parse query page: %w", err)
	}
	if page.Resources == nil {
		page.Resources = []queryHit{}
	}
	return page, nil
}

// mustQueryPage runs resource query and fails t on fleetctl or parse errors.
func mustQueryPage(t *testing.T, f *harness.Fixture, req resourceQueryRequest) queryPage {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	parsed, res := runResourceQuery(t, f, req)
	g.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
	g.Expect(parsed.parseErr).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
	return parsed.page
}

// parsedQueryPage is a decoded query page plus any JSON parse error.
type parsedQueryPage struct {
	page     queryPage
	parseErr error
}

// runResourceQuery runs fleetctl resource query. On a fleetctl error, page is
// zero and parseErr is nil.
func runResourceQuery(t *testing.T, f *harness.Fixture, req resourceQueryRequest) (parsedQueryPage, harness.FleetctlResult) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), queryCommandTimeout)
	defer cancel()
	res := f.Run(ctx, req.args()...)
	if res.Err != nil {
		return parsedQueryPage{}, res
	}
	page, err := parseQueryPage(res.Stdout)
	return parsedQueryPage{page: page, parseErr: err}, res
}

// waitForQuery polls resource query until check passes, then returns the last hits.
func waitForQuery(t *testing.T, f *harness.Fixture, req resourceQueryRequest, check func(gm gomega.Gomega, hits []queryHit)) []queryHit {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(f).NotTo(gomega.BeNil())
	log := newPollLog(t)
	var found []queryHit
	g.Eventually(func(gm gomega.Gomega) {
		parsed, res := runResourceQuery(t, f, req)
		if res.Err != nil {
			log.logf("query: %s", fleetctlDetail(res))
		}
		gm.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
		gm.Expect(parsed.parseErr).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
		hits := parsed.page.Resources
		log.logf("query %s n=%d token=%t", clip(req.filter, 80), len(hits), parsed.page.NextPageToken != "")
		check(gm, hits)
		found = hits
	}).WithTimeout(kindIndexWaitTimeout).WithPolling(queryPollInterval).Should(gomega.Succeed())
	return found
}

// observation unmarshals the Get/List body and returns its observation object.
func (h queryHit) observation() (queryObservation, error) {
	var body struct {
		Observation queryObservation `json:"observation"`
	}
	if err := json.Unmarshal(h.Resource, &body); err != nil {
		return queryObservation{}, fmt.Errorf("parse query resource body: %w", err)
	}
	return body.Observation, nil
}

// observationOrZero returns the observation object, or zero if the body cannot be parsed.
func (h queryHit) observationOrZero() queryObservation {
	obs, _ := h.observation()
	return obs
}

// observationKind returns observation.kind, or "" if the body cannot be parsed.
func observationKind(hit queryHit) string {
	return hit.observationOrZero().Kind
}

// observationMetaName returns observation.metadata.name, or "" if the body cannot be parsed.
func observationMetaName(hit queryHit) string {
	return hit.observationOrZero().Metadata.Name
}

// observationMetaNames returns metadata.name from each hit that parses with a non-empty name.
func observationMetaNames(hits []queryHit) []string {
	names := make([]string, 0, len(hits))
	for _, h := range hits {
		if n := observationMetaName(h); n != "" {
			names = append(names, n)
		}
	}
	return names
}

// kindNodeLeafNames returns the Node id from each kind Node envelope name
// (//kind.fleetshift.io/nodes/{id}). Names without that prefix or with an
// empty leaf are omitted.
func kindNodeLeafNames(hits []queryHit) []string {
	names := make([]string, 0, len(hits))
	for _, h := range hits {
		if !strings.HasPrefix(h.Name, kindNodeEnvelopePrefix) {
			continue
		}
		if leaf := strings.TrimPrefix(h.Name, kindNodeEnvelopePrefix); leaf != "" {
			names = append(names, leaf)
		}
	}
	return names
}

// extractedString returns observation.extracted[key] as a string, or "" if missing or not a string.
func extractedString(hit queryHit, key string) string {
	s, _ := hit.observationOrZero().Extracted[key].(string)
	return s
}

// kubernetesObjectNamePrefix is the resource-query name prefix for kubernetes Objects in clusterID.
func kubernetesObjectNamePrefix(clusterID string) string {
	return "//kubernetes.fleetshift.io/clusters/" + url.PathEscape(clusterID) + "/"
}

// kindClusterEnvelopeName is the resource-query name for a Kind cluster.
func kindClusterEnvelopeName(clusterID string) string {
	return "//kind.fleetshift.io/" + jsonClusterName(clusterID)
}

// kubernetesObjectsInClusterFilter is a CEL filter for kubernetes Objects whose name is under clusterID.
func kubernetesObjectsInClusterFilter(clusterID string) string {
	return fmt.Sprintf(`resourceType == %q && name.startsWith(%q)`,
		kubernetesObjectQueryType, kubernetesObjectNamePrefix(clusterID))
}

// kubernetesObjectKindFilter is kubernetesObjectsInClusterFilter plus observation.kind.
func kubernetesObjectKindFilter(clusterID, kind string) string {
	return kubernetesObjectsInClusterFilter(clusterID) +
		fmt.Sprintf(` && resource.observation.kind == %q`, kind)
}

// kubernetesObjectConfigMapFilter is a CEL filter for a named ConfigMap Object in clusterID.
func kubernetesObjectConfigMapFilter(clusterID, namespace, name string) string {
	return kubernetesObjectKindFilter(clusterID, "ConfigMap") +
		fmt.Sprintf(` && resource.observation.metadata.namespace == %q && resource.observation.metadata.name == %q`,
			namespace, name)
}

// kindClusterReadyFilter is a CEL filter for an ACTIVE Kind cluster with Ready=True.
func kindClusterReadyFilter(clusterID string) string {
	return fmt.Sprintf(
		`resourceType == %q && resource.name == %q && resource.state == %q && resource.conditions["Ready"].status == %q`,
		kindClusterQueryType, jsonClusterName(clusterID), clusterStateActive, clusterReadyTrue,
	)
}

// kindNodeInClusterFilter is a CEL filter for kind.fleetshift.io/Node observations in clusterID.
func kindNodeInClusterFilter(clusterID string) string {
	return fmt.Sprintf(`resourceType == %q && resource.observation.cluster == %q`,
		kindNodeQueryType, jsonClusterName(clusterID))
}

// deniedKubernetesGVRFilter is a CEL filter for kubernetes Objects whose GVR is in the denied high-volume set.
func deniedKubernetesGVRFilter() string {
	return fmt.Sprintf(
		`resourceType == %q && resource.observation.gvr.resource in ["events","leases","endpoints","endpointslices","componentstatuses"]`,
		kubernetesObjectQueryType,
	)
}

// kubernetesObjectInCluster reports whether hit is a kubernetes Object instance
// in clusterID (type, cluster name prefix, apiResources path, non-empty /objects/ leaf).
func kubernetesObjectInCluster(hit queryHit, clusterID string) bool {
	if hit.ResourceType != kubernetesObjectQueryType {
		return false
	}
	if !strings.HasPrefix(hit.Name, kubernetesObjectNamePrefix(clusterID)) {
		return false
	}
	if !strings.Contains(hit.Name, "/apiResources/") {
		return false
	}
	const leaf = "/objects/"
	i := strings.LastIndex(hit.Name, leaf)
	return i >= 0 && i+len(leaf) < len(hit.Name)
}

// namesOutsideCluster returns hit names that are not kubernetes Objects in clusterID.
func namesOutsideCluster(hits []queryHit, clusterID string) []string {
	var names []string
	for _, h := range hits {
		if !kubernetesObjectInCluster(h, clusterID) {
			names = append(names, h.Name)
		}
	}
	return names
}

// WaitForIndexedKubernetesObjects polls resource query until this Kind cluster
// has Node objects with extracted kubeletVersion, default and kube-system
// Namespaces, and at least one ConfigMap. Denied high-volume GVRs must not appear.
func WaitForIndexedKubernetesObjects(t *testing.T, f *harness.Fixture, clusterID string) {
	t.Helper()
	waitForQuery(t, f, inspectQuery(kubernetesObjectKindFilter(clusterID, "Node")), func(gm gomega.Gomega, hits []queryHit) {
		gm.Expect(hits).NotTo(gomega.BeEmpty(), "want indexed Node objects")
		gm.Expect(namesOutsideCluster(hits, clusterID)).To(gomega.BeEmpty())
		for _, hit := range hits {
			gm.Expect(extractedString(hit, "kubeletVersion")).NotTo(gomega.BeEmpty(),
				"node %s missing extracted.kubeletVersion", hit.Name)
		}
	})
	waitForQuery(t, f, inspectQuery(kubernetesObjectKindFilter(clusterID, "Namespace")), func(gm gomega.Gomega, hits []queryHit) {
		names := observationMetaNames(hits)
		gm.Expect(names).To(gomega.ContainElements("default", "kube-system"),
			"indexed namespaces=%v", names)
		gm.Expect(namesOutsideCluster(hits, clusterID)).To(gomega.BeEmpty())
	})
	waitForQuery(t, f, inspectQuery(kubernetesObjectKindFilter(clusterID, "ConfigMap")), func(gm gomega.Gomega, hits []queryHit) {
		gm.Expect(hits).NotTo(gomega.BeEmpty(), "want indexed ConfigMap objects")
		gm.Expect(namesOutsideCluster(hits, clusterID)).To(gomega.BeEmpty())
	})
	assertDeniedKubernetesGVRsAbsent(t, f)
}

// AssertKindClusterQueryMatchesGet polls resource query until one ACTIVE,
// Ready=True Kind cluster hit exists for clusterID, then checks that the hit
// name, state, and Ready status match resource get.
func AssertKindClusterQueryMatchesGet(t *testing.T, f *harness.Fixture, clusterID string) {
	t.Helper()
	hits := waitForQuery(t, f, inspectQuery(kindClusterReadyFilter(clusterID)), func(gm gomega.Gomega, hits []queryHit) {
		gm.Expect(hits).To(gomega.HaveLen(1), "want exactly one ready Kind cluster hit")
		gm.Expect(hits[0].Name).To(gomega.Equal(kindClusterEnvelopeName(clusterID)))
		gm.Expect(hits[0].ResourceType).To(gomega.Equal(kindClusterQueryType))
	})

	g := gomega.NewWithT(t)
	ctx, cancel := context.WithTimeout(context.Background(), clusterCommandTimeout)
	defer cancel()
	res := f.Run(ctx, "resource", "get", kindClusterType, clusterID)
	g.Expect(res.Err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))
	got, err := parseCluster(res.Stdout)
	g.Expect(err).NotTo(gomega.HaveOccurred(), fleetctlDetail(res))

	body, err := parseCluster(string(hits[0].Resource))
	g.Expect(err).NotTo(gomega.HaveOccurred(), "query resource body")
	g.Expect(body.Name).To(gomega.Equal(got.Name))
	g.Expect(body.State).To(gomega.Equal(got.State))
	g.Expect(body.Conditions["Ready"].Status).To(gomega.Equal(got.Conditions["Ready"].Status))
}

// WaitForIndexedConfigMap polls resource query until namespace/test-config is
// indexed as a kubernetes Object for this cluster.
func WaitForIndexedConfigMap(t *testing.T, f *harness.Fixture, clusterID, namespace string) {
	t.Helper()
	g := gomega.NewWithT(t)
	g.Expect(namespace).NotTo(gomega.BeEmpty())
	waitForQuery(t, f, inspectQuery(kubernetesObjectConfigMapFilter(clusterID, namespace, configMapName)), func(gm gomega.Gomega, hits []queryHit) {
		gm.Expect(hits).To(gomega.HaveLen(1),
			"want ConfigMap %s/%s indexed once", namespace, configMapName)
		gm.Expect(namesOutsideCluster(hits, clusterID)).To(gomega.BeEmpty())
		obs, err := hits[0].observation()
		gm.Expect(err).NotTo(gomega.HaveOccurred(), hits[0].Name)
		gm.Expect(obs.Kind).To(gomega.Equal("ConfigMap"))
		gm.Expect(obs.Metadata.Name).To(gomega.Equal(configMapName))
		gm.Expect(obs.Metadata.Namespace).To(gomega.Equal(namespace))
	})
}

// AssertKubernetesObjectQueryPaginates walks kubernetes Object hits for
// this cluster with a small page size, requiring more than one page, no
// duplicate names, that each page honors page-size, and that a page
// token is bound to its filter.
func AssertKubernetesObjectQueryPaginates(t *testing.T, f *harness.Fixture, clusterID string) {
	t.Helper()
	g := gomega.NewWithT(t)
	hits, firstNext := collectQueryPages(t, f, resourceQueryRequest{
		filter:   kubernetesObjectsInClusterFilter(clusterID),
		pageSize: queryPaginationPageSize,
		orderBy:  queryPaginationOrderBy,
	})
	g.Expect(firstNext).NotTo(gomega.BeEmpty(),
		"want more than one page (page-size %d, got %d hits)",
		queryPaginationPageSize, len(hits))
	g.Expect(len(hits)).To(gomega.BeNumerically(">", int(queryPaginationPageSize)),
		"want more hits than one page, got %d", len(hits))
	g.Expect(namesOutsideCluster(hits, clusterID)).To(gomega.BeEmpty())
	assertQueryPageTokenRejectsFilterChange(t, f, firstNext, queryPaginationOrderBy)
}

// WaitForDualIndexedNodes polls resource query until this cluster has both a
// kind.fleetshift.io/Node and a kubernetes.fleetshift.io/Object Node for the
// same node names. Kind Node hits must carry observation.cluster for clusterID.
func WaitForDualIndexedNodes(t *testing.T, f *harness.Fixture, clusterID string) {
	t.Helper()
	wantCluster := jsonClusterName(clusterID)
	kindHits := waitForQuery(t, f, inspectQuery(kindNodeInClusterFilter(clusterID)), func(gm gomega.Gomega, hits []queryHit) {
		gm.Expect(hits).NotTo(gomega.BeEmpty(), "want kind.fleetshift.io/Node")
		for _, hit := range hits {
			gm.Expect(hit.ResourceType).To(gomega.Equal(kindNodeQueryType))
			gm.Expect(hit.Name).To(gomega.HavePrefix(kindNodeEnvelopePrefix))
			obs, err := hit.observation()
			gm.Expect(err).NotTo(gomega.HaveOccurred(), hit.Name)
			gm.Expect(obs.Cluster).To(gomega.Equal(wantCluster))
		}
	})
	k8sHits := waitForQuery(t, f, inspectQuery(kubernetesObjectKindFilter(clusterID, "Node")), func(gm gomega.Gomega, hits []queryHit) {
		gm.Expect(hits).NotTo(gomega.BeEmpty(), "want kubernetes.fleetshift.io/Object Node")
		gm.Expect(namesOutsideCluster(hits, clusterID)).To(gomega.BeEmpty())
		for _, hit := range hits {
			gm.Expect(observationKind(hit)).To(gomega.Equal("Node"))
		}
	})
	g := gomega.NewWithT(t)
	g.Expect(observationMetaNames(k8sHits)).To(gomega.ConsistOf(kindNodeLeafNames(kindHits)),
		"kind Node names vs kubernetes Object metadata.name")
}

// assertDeniedKubernetesGVRsAbsent fails t if resource query returns any
// kubernetes Object in the denied high-volume GVR set.
func assertDeniedKubernetesGVRsAbsent(t *testing.T, f *harness.Fixture) {
	t.Helper()
	g := gomega.NewWithT(t)
	page := mustQueryPage(t, f, inspectQuery(deniedKubernetesGVRFilter()))
	g.Expect(page.Resources).To(gomega.BeEmpty(),
		"denied GVRs in query: %s", queryHitNames(page.Resources))
}

// assertQueryPageTokenRejectsFilterChange fails t unless resource query with
// token and a different filter errors with a filter-mismatch message.
func assertQueryPageTokenRejectsFilterChange(t *testing.T, f *harness.Fixture, token, orderBy string) {
	t.Helper()
	g := gomega.NewWithT(t)
	_, res := runResourceQuery(t, f, resourceQueryRequest{
		filter:    fmt.Sprintf(`resourceType == %q`, kindClusterQueryType),
		pageSize:  queryPaginationPageSize,
		pageToken: token,
		orderBy:   orderBy,
	})
	g.Expect(res.Err).To(gomega.HaveOccurred(), "page token must not resume a different filter")
	combined := strings.ToLower(res.Stderr + " " + errString(res.Err))
	g.Expect(combined).To(gomega.ContainSubstring("does not match"), fleetctlDetail(res))
}

// collectQueryPages walks resource query pages for req until NextPageToken is
// empty. Duplicate names fail t. Caps at queryPaginationMaxPages. firstNext is
// the first page's NextPageToken (empty when the result fits on one page).
// When req.pageSize > 0, a page with a next token must have exactly that many
// hits and a final page must not exceed it.
func collectQueryPages(t *testing.T, f *harness.Fixture, req resourceQueryRequest) (hits []queryHit, firstNext string) {
	t.Helper()
	return collectQueryPagesWith(t, req, func(r resourceQueryRequest) queryPage {
		return mustQueryPage(t, f, r)
	})
}

// collectQueryPagesWith is collectQueryPages using fetch for each page.
func collectQueryPagesWith(t *testing.T, req resourceQueryRequest, fetch func(resourceQueryRequest) queryPage) (hits []queryHit, firstNext string) {
	t.Helper()
	g := gomega.NewWithT(t)
	seen := make(map[string]struct{})
	token := ""
	for pageNum := 1; pageNum <= queryPaginationMaxPages; pageNum++ {
		req.pageToken = token
		page := fetch(req)
		assertQueryPageSize(t, req.pageSize, pageNum, page)
		if pageNum == 1 {
			firstNext = page.NextPageToken
		}
		for _, hit := range page.Resources {
			if _, dup := seen[hit.Name]; dup {
				t.Fatalf("duplicate query name %q on page %d", hit.Name, pageNum)
			}
			seen[hit.Name] = struct{}{}
			hits = append(hits, hit)
		}
		if page.NextPageToken == "" {
			g.Expect(hits).NotTo(gomega.BeEmpty())
			return hits, firstNext
		}
		token = page.NextPageToken
	}
	t.Fatalf("query did not exhaust after %d pages (%d hits)", queryPaginationMaxPages, len(hits))
	return nil, firstNext
}

// assertQueryPageSize fails t unless page honors pageSize. pageSize 0 means
// server default and is not checked.
func assertQueryPageSize(t *testing.T, pageSize int32, pageNum int, page queryPage) {
	t.Helper()
	if pageSize <= 0 {
		return
	}
	g := gomega.NewWithT(t)
	n := len(page.Resources)
	if page.NextPageToken != "" {
		g.Expect(n).To(gomega.Equal(int(pageSize)),
			"page %d has %d hits and a next token, want exactly page-size %d",
			pageNum, n, pageSize)
		return
	}
	g.Expect(n).To(gomega.BeNumerically("<=", int(pageSize)),
		"page %d has %d hits, want <= page-size %d", pageNum, n, pageSize)
}

// queryHitNames joins hit names with commas for assertion messages.
func queryHitNames(hits []queryHit) string {
	names := make([]string, len(hits))
	for i, h := range hits {
		names[i] = h.Name
	}
	return strings.Join(names, ",")
}
