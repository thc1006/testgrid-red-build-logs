# Notes for coding agents

## README.md

README.md is written for people, and its wording is settled. Change it only
when the code changes what it says, or adds something a reader needs to know,
and then change only the sentences involved. Check each changed sentence
against the code. Do not restyle, reorder or shorten the rest.

Keep the `## Limits` heading: INDEX.md links to it, and a test checks it.

`tool/test_docs.py` fails unless each number README.md writes in digits is
bound in `ReadmeNumbers` or listed in its `UNBOUND`, each raw HTML tag of the
docs `DOCS` lists is in `Tags.SHOWN`, each HTML comment of those docs is
whole on one line, and each file they link to outside `tool/` and `archive/`
is in `READ` and in the paths of `.github/workflows/test.yml`.

## The workflow's token

Of the steps that run the tool, only the upload step and the commit step get
the token. Keep `tool/upload_assets.py` free of imports from the fetch, and
immutable releases off (Settings, Releases): the runs add assets to a
published release.
