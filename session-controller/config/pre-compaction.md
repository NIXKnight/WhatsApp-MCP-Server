Produce one strict CheckpointV1 JSON object from the finalized settled session context.
Populate only these CheckpointV1 fields: `version`, `canonicalChatId`, `lastCompletedSequence`, `policyVersion`, `templateVersion`, `createdAt`, `goals`, `decisions`, `constraints`, `verifiedFacts`, `pendingWork`, `approvalBoundaries`, `nextSteps`, and `confirmedToolOutcomes`.
Preserve only event-relevant values in goals, decisions, constraints, verified facts, pending work, approval boundaries, and executable next steps.
Copy the supplied version, canonicalChatId, lastCompletedSequence, policyVersion, templateVersion, createdAt, and confirmedToolOutcomes exactly.
Persona, style, voice, engagement defaults, and profile text are reloaded from system configuration. Do not copy, summarize, infer, reinterpret, or mutate them into checkpoint facts. Preserve a behavioral instruction only when it is an event-specific operator decision or approval boundary, and record only that event-relevant decision.
Do not invent facts, outcomes, authority, continuity, or profile changes. Do not turn examples, source corpus material, historical persona text, or unverified tool effects into semantic state.
Do not include prose outside JSON. Do not include message transcripts, raw prompts, tool arguments/results, credentials, tokens, private keys, connection URLs, filesystem paths, or inferred facts.
