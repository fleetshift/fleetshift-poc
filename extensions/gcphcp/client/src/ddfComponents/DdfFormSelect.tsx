import { useFieldApi } from "@data-driven-forms/react-form-renderer";
import {
  FormGroup,
  FormHelperText,
  FormSelect,
  FormSelectOption,
  HelperText,
  HelperTextItem,
} from "@patternfly/react-core";

interface SelectOption {
  label: string;
  value: string;
}

interface DdfFormSelectProps {
  name: string;
  label?: string;
  isRequired?: boolean;
  helperText?: string;
  options?: SelectOption[];
  [key: string]: unknown;
}

export default function DdfFormSelect(props: DdfFormSelectProps) {
  const {
    input,
    meta,
    label,
    isRequired,
    helperText,
    options = [],
    ...rest
  } = useFieldApi(props);

  const isError = meta.touched && meta.error;

  return (
    <FormGroup
      label={label}
      isRequired={isRequired}
      fieldId={input.name}
      {...rest.FormGroupProps}
    >
      <FormSelect
        id={input.name}
        value={input.value ?? ""}
        onChange={(_e, val) => input.onChange(val)}
        onBlur={() => input.onBlur()}
        validated={isError ? "error" : "default"}
      >
        {options.map((o) => (
          <FormSelectOption key={o.value} value={o.value} label={o.label} />
        ))}
      </FormSelect>
      {(isError || helperText) && (
        <FormHelperText>
          <HelperText>
            <HelperTextItem variant={isError ? "error" : "default"}>
              {isError ? meta.error : helperText}
            </HelperTextItem>
          </HelperText>
        </FormHelperText>
      )}
    </FormGroup>
  );
}
