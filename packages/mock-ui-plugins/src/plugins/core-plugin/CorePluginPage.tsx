import { type ReactNode, lazy, Suspense } from "react";

const ClustersPage = lazy(() => import("./ClustersPage"));
const ClusterDetailPage = lazy(() => import("./ClusterDetailPage"));
import { Routes, Route, useNavigate, Link } from "react-router-dom";
import {
  Breadcrumb,
  BreadcrumbItem,
  Title,
  Card,
  CardHeader,
  CardTitle,
  CardBody,
  Gallery,
  Label,
} from "@patternfly/react-core";

interface ComponentCard {
  title: string;
  slug: string;
  description: string;
  status: "planned" | "in-progress" | "done";
  element?: ReactNode;
}

const components: ComponentCard[] = [
  {
    title: "Clusters",
    slug: "clusters",
    description: "Manage and monitor your kind clusters across the fleet.",
    status: "in-progress",
    element: (
      <Suspense>
        <ClustersPage />
      </Suspense>
    ),
  },
];

const statusColor = {
  planned: "blue" as const,
  "in-progress": "orange" as const,
  done: "green" as const,
};

function SubPage({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div>
      <Breadcrumb style={{ marginBottom: "var(--pf-t--global--spacer--md)" }}>
        <BreadcrumbItem>
          <Link to="/core-plugin">Core Plugin</Link>
        </BreadcrumbItem>
        <BreadcrumbItem isActive>{title}</BreadcrumbItem>
      </Breadcrumb>
      {children}
    </div>
  );
}

function Placeholder({ title }: { title: string }) {
  return (
    <div>
      <Title headingLevel="h1">{title}</Title>
      <p>Component not yet implemented.</p>
    </div>
  );
}

function ComponentGallery() {
  const navigate = useNavigate();

  return (
    <div>
      <Title
        headingLevel="h1"
        style={{ marginBottom: "var(--pf-t--global--spacer--lg)" }}
      >
        Core Plugin
      </Title>
      <Gallery hasGutter minWidths={{ default: "300px" }}>
        {components.map((c) => (
          <Card
            key={c.slug}
            isFullHeight
            isClickable
            isSelectable
            onClick={() => navigate(c.slug)}
            style={{ cursor: "pointer" }}
          >
            <CardHeader
              selectableActions={{ selectableActionAriaLabel: "actions" }}
              actions={{
                actions: (
                  <Label color={statusColor[c.status]}>{c.status}</Label>
                ),
              }}
            >
              <CardTitle>{c.title}</CardTitle>
            </CardHeader>
            <CardBody>{c.description}</CardBody>
          </Card>
        ))}
      </Gallery>
    </div>
  );
}

export default function CorePluginPage() {
  return (
    <Routes>
      <Route index element={<ComponentGallery />} />
      {components.map((c) => (
        <Route
          key={c.slug}
          path={c.slug}
          element={
            <SubPage title={c.title}>
              {c.element ?? <Placeholder title={c.title} />}
            </SubPage>
          }
        />
      ))}
      <Route
        path="clusters/:deploymentId"
        element={
          <Suspense>
            <ClusterDetailPage />
          </Suspense>
        }
      />
    </Routes>
  );
}
