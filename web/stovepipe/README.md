# Stovepipe web UI

A framework-neutral React UI with a queue request list and a change page containing project status, results and retained history. The host supplies queue/project configuration, a structural `StovepipeService` client, authentication, transport, logging and OpenTelemetry providers.

Logging uses a synchronous `onLog` callback passed on each `handle` request. Bind it to the host's request-scoped logger; it receives severity, message, and structured metadata containing the original `error`, correlation `requestId`, and page `path`. Omitting it is silent, and callback exceptions do not change page results. The library has no logging-library dependency.

## Integration

Use the server entry's `createStovepipeWeb` to load a page and render its model with `StovepipeApp`. Mount beneath `WebNavigationProvider` when the host supplies client navigation and refresh. Without it, links and refresh use ordinary document navigation. Import `@submitqueue/web-stovepipe/styles.css`, which includes the common native styles from `@submitqueue/web-platform`.

## Routes and data

Change pages use `/<queue>/change/<percent-escaped-uri>` and look up status with `GetProjectStatusByURI`. History uses the request ID returned by that lookup. The `/request/` path is reserved for a future ID-based view. Queue pagination uses the backend's opaque continuations, and the latest URL carries no time window.

Project results request ten configured project IDs per page beside the request status, with history below. Only entries returned by the backend are displayed; a page can have fewer than ten results or be empty. The raw JSON link collects the returned project entries across all backend pages, independent of display pagination. The reference host serializes only the projects array as protobuf JSON. Configured projects are not an inventory of every project in a repository.

## Refresh and presentation

Refresh is manual by default, with optional ten-second polling. Failed refreshes retain successful sections on the same route; changing routes clears them. Timestamps use UTC. Project completeness and lifecycle labels come from the backend, and missing results remain distinct from recorded zero/green.
