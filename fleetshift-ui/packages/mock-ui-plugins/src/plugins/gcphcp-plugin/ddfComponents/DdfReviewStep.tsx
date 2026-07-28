import { FormSpy } from "@data-driven-forms/react-form-renderer";

import type { GcpHcpFormData } from "../CreateGcpHcpWizard";
import ReviewStep from "../ReviewStep";

export default function DdfReviewStep() {
  return (
    <FormSpy subscription={{ values: true }}>
      {({ values }) => <ReviewStep formData={values as GcpHcpFormData} />}
    </FormSpy>
  );
}
