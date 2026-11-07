# Writing memories the judge keeps

When the optional memory gate is on ([write-gate.md](write-gate.md)), SAGE's judge reads only the memory text. It sees no type, author, domain or chat. It asks whether the memory will still be true and useful outside the conversation it came from (and, when evidence is attached, whether the evidence supports it as stated). Write every memory so a stranger could read it a month from now and still act on it.

## The shape

**Who or what** + **the statement** + (optional) **why**, in one or two plain sentences.

- Name the system, team or person: "the billing service", "the platform team", not "it" or "this".
- State the fact, decision or rule. Don't describe the chat.
- Add the reason when there is one. A reason makes a memory clearer to the judge and to a later reader.

## By kind

| Kind | Write it like | Not like |
|---|---|---|
| Fact | "The analytics database is about 4 TB." | "As I said above, it's about 4 TB." |
| Decision / preference | "The user chose a monthly release cadence for the mobile app so QA can test each build." | "The user said no to that." / "The user wants it shorter." |
| Rule / method | "Run migrations on staging first, inside a transaction." | "Remember to do that next time." |
| Task / backlog item | "Add rate limiting to the password reset endpoint; it is being abused for spam." | "TODO: do the rate limit thing." |
| Bug | "Bug: CSV export drops rows with non-ASCII customer names." | "Found the bug we talked about." |

## Don't store these (the judge rejects them, by design)

- Notes about the session itself: "Turn ended", "session started", "the attachment didn't load", "context was truncated".
- Steps that only matter right now: "re-send the screenshot", "answer the follow-up", "wait for the user".
- References that need the chat to make sense: "the file above", "what we discussed", "this one".

## Avoid labels and shorthand

Don't start a memory with `TODO:`, `Task:`, `[planned]` or similar. Status belongs in the task's own fields, not in the text. Write the work itself in a full sentence.

## One memory, one point

Split a list of unrelated facts into separate memories. A judge can keep one point and drop another, but not half of one memory.
