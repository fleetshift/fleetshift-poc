import {
  Bullseye,
  Button,
  Content,
  Divider,
  Icon,
  Stack,
  StackItem,
} from "@patternfly/react-core";
import ChevronDownIcon from "@patternfly/react-icons/dist/dynamic/icons/chevron-down-icon";

export default function WelcomeFooter() {
  return (
    <Stack hasGutter>
      <StackItem>
        <Bullseye>
          <Button
            variant="link"
            icon={
              <Icon isInline>
                <ChevronDownIcon />
              </Icon>
            }
            iconPosition="right"
          >
            Select cluster type to create or import a cluster
          </Button>
        </Bullseye>
      </StackItem>
      <StackItem>
        <Divider />
      </StackItem>
      <StackItem>
        <Bullseye>
          <Content component="small">Not ready to create add clusters?</Content>
        </Bullseye>
      </StackItem>
      <StackItem>
        <Bullseye>
          <Button variant="link">Skip and go to dashboard</Button>
        </Bullseye>
      </StackItem>
    </Stack>
  );
}
