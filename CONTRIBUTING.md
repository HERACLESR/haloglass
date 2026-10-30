# Contributing

Thanks for helping! This project is a **single-file** read-only monitoring dashboard, so a few conventions keep it maintainable.

## Issues

- One bug / one feature per issue.
- For bugs, include: browser + version, dashboard language & theme, what you expected vs. what you saw, and (if display-related) a screenshot.
- For metric-semantics questions, check the FAQ in [README](README.md#metric-faq) first — several "bugs" are actually intentional metric definitions (e.g. Reserved is not a subset of Used).

## Pull Requests

- Keep the diff minimal and focused; describe *why* before *how*.
- UI changes: attach before/after screenshots in **both themes**, and update **all four languages** (`zh-CN`, `en`, `ko`, `de`) for any user-visible string.
- Do not weaken the security invariants: loopback/Tailscale-only binds, Host whitelist (421 on mismatch), read-only `GET` access to the engine, no query strings in logs.

## DCO

We suggest signing off commits to certify the Developer Certificate of Origin:

```bash
git commit -s
```

## Code style

- **Stay single-file.** Everything ships in `dash.py`; no new modules, no third-party dependencies (Python 3 stdlib only).
- The file is organized by **comment banners into zones** — presentation layer (HTML/CSS/JS), collection layer (engine polling), and the **S2 security zone** (binds, Host whitelist, response headers). Keep changes inside the zone they belong to and don't move code across zones casually. Zone banner comments in the source are in Chinese; the zone map above is the English reference.
- Follow the existing naming and formatting of the zone you touch; comments explaining *why* are welcome, comments restating *what* are not.

## License

By contributing you agree that your contributions will be licensed under the [MIT License](LICENSE).
