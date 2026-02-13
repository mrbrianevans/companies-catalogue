# Historical Events API Documentation

This service provides access to historical events from the Companies House Streaming API.

## Starting timepoints

To find out the timepoints available for a given stream, use the `/:stream/timepoints` endpoint.

## Event Endpoints

This service aims to follow the same API structure as the official Streaming API from Companies House, in order to make it easy to use with existing integrations and code.

The timepoint parameter is mandatory.

The paths available are:

```js
[
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
```

Request URL format:

```
GET /:stream?timepoint=:timepoint
```

For example,

```http request
GET /charges?timepoint=3508711
```

Events will be streamed in JSON format, with a new line separating each event.

## Testing

To test your connection to the service, request the range of timepoints available, eg

```http request
GET /charges/timepoints
```

Response:

```json
{ "min": 3508711, "max": 3589278 }
```

Start 100 events before the max timepoint:

```http request
GET /charges?timepoint=3589178
```

And you should receive 100 events in JSON format.
