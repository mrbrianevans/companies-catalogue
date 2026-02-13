export const streams = [
  "companies",
  "filings",
  "officers",
  "persons-with-significant-control",
  "charges",
  "insolvency-cases",
  "disqualified-officers",
  "company-exemptions",
  "persons-with-significant-control-statements",
];
export const makeError = (code: number, message: string) =>
  Response.json({ error: message }, { status: code });
