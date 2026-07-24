import {
  EmptyState,
  EmptyStateBody,
  PageSection,
  Title,
} from "@patternfly/react-core";

const CompliancePage = () => {
  return (
    <PageSection>
      <Title headingLevel="h1" className="pf-v6-u-mb-lg">
        Compliance
      </Title>
      <EmptyState headingLevel="h2" titleText="Coming soon">
        <EmptyStateBody>
          Track regulatory compliance and audit readiness across your fleet.
          Monitor policy adherence and generate compliance reports.
        </EmptyStateBody>
      </EmptyState>
    </PageSection>
  );
};

export default CompliancePage;
