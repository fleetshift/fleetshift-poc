import { Routes, Route, useNavigate } from "react-router-dom";
import {
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
}

const components: ComponentCard[] = [
  {
    title: "Cluster Overview",
    slug: "cluster-overview",
    description:
      "Summary view of all managed clusters with status indicators and quick actions.",
    status: "planned",
  },
  {
    title: "Cluster Detail",
    slug: "cluster-detail",
    description:
      "Detailed cluster view with node pools, status, and lifecycle actions.",
    status: "planned",
  },
  {
    title: "Create Cluster Wizard",
    slug: "create-cluster",
    description:
      "Step-by-step wizard for provisioning new clusters with template selection.",
    status: "planned",
  },
  {
    title: "Node Pool Management",
    slug: "node-pools",
    description: "Scale, add, or remove node pools within a cluster.",
    status: "planned",
  },
  {
    title: "Cluster Credentials",
    slug: "credentials",
    description:
      "Download kubeconfig and view access credentials for a cluster.",
    status: "planned",
  },
  {
    title: "Activity & Events",
    slug: "activity",
    description:
      "Timeline of cluster lifecycle events, provisioning progress, and alerts.",
    status: "planned",
  },
];

const statusColor = {
  planned: "blue" as const,
  "in-progress": "orange" as const,
  done: "green" as const,
};

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
        Day One
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

export default function DayOnePage() {
  return (
    <Routes>
      <Route index element={<ComponentGallery />} />
      {components.map((c) => (
        <Route
          key={c.slug}
          path={c.slug}
          element={<Placeholder title={c.title} />}
        />
      ))}
    </Routes>
  );
}
