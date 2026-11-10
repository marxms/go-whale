## Summary

<!-- What behaviour changes and why. -->

## Linked issue

<!-- Closes #... -->

## Behaviour before and after

<!-- The observable difference. For a venue change, name the capability matrix row. -->

## Test proof

<!-- The test that fails without this change and passes with it. -->

## Gate output

```text
# the tail of `make verify`
```

## Checklist

- [ ] `make verify` passes
- [ ] A test fails without this change and passes with it
- [ ] The change stays inside its scope; no unrelated files changed
- [ ] No new dependency without a recorded reason
- [ ] Docs, configuration reference or ADR updated when the contract changed
- [ ] No credentials, private host names or production data in the diff or logs
- [ ] For a new venue: the certification checklist in `docs/adapters.md` is satisfied and the evidence is linked
