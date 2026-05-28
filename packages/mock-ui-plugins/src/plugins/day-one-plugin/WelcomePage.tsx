import { usePluginNavigate } from "@fleetshift/common";
import {
  Bullseye,
  Icon,
  Split,
  SplitItem,
  Stack,
  StackItem,
  Title,
} from "@patternfly/react-core";
import BrainIcon from "@patternfly/react-icons/dist/dynamic/icons/brain-icon";
import CodeIcon from "@patternfly/react-icons/dist/dynamic/icons/code-icon";
import ServerIcon from "@patternfly/react-icons/dist/dynamic/icons/server-icon";

import WorkloadCard, { type WorkloadOption } from "./WorkloadCard";
import ScalingCard from "./ScalingCard";
import WelcomeFooter from "./WelcomeFooter";

import "./WelcomePage.scss";

const workloads: WorkloadOption[] = [
  {
    title: "Virtualization",
    description: "Optimized for running VMs alongside containers.",
    badgeVariant: "blue",
    icon: (
      <Icon size="xl">
        <ServerIcon />
      </Icon>
    ),
  },
  {
    title: "AI & Machine Learning",
    description: "Accelerated nodes for training and model serving.",
    badgeVariant: "red",
    icon: (
      <Icon size="xl">
        <BrainIcon />
      </Icon>
    ),
  },
  {
    title: "Application Development",
    description: "Standard multi-tenant clusters for microservices.",
    badgeVariant: "green",
    icon: (
      <Icon size="xl">
        <CodeIcon />
      </Icon>
    ),
  },
];

export default function WelcomePage() {
  const dayOne = usePluginNavigate("day-one-plugin", "DayOnePage");

  return (
    <Stack hasGutter className="day-one-welcome">
      <StackItem className="pf-v6-u-mb-lg">
        <Bullseye>
          <Stack hasGutter>
            <StackItem>
              <Title headingLevel="h1" size="3xl">
                Welcome to OpenShift Management Engine
              </Title>
            </StackItem>
            <StackItem>
              <p className="pf-v6-u-font-size-lg">
                Let's create your first clusters. What kind of workload are you
                looking to run today?
              </p>
            </StackItem>
          </Stack>
        </Bullseye>
      </StackItem>

      <StackItem>
        <Title headingLevel="h2" size="lg">
          What workload are you looking to run today?
        </Title>
      </StackItem>

      <StackItem className="pf-v6-u-mb-lg">
        <Split hasGutter>
          {workloads.map((w) => (
            <SplitItem key={w.title} isFilled>
              <WorkloadCard workload={w} />
            </SplitItem>
          ))}
        </Split>
      </StackItem>

      <StackItem className="pf-v6-u-mb-md">
        <Title headingLevel="h2" size="lg">
          Scaling & Infrastructure
        </Title>
      </StackItem>

      <StackItem>
        <ScalingCard onSetup={() => dayOne.navigate("create-cluster")} />
      </StackItem>

      <StackItem>
        <WelcomeFooter />
      </StackItem>
    </Stack>
  );
}
