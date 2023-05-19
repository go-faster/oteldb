# Contributing

## Conventional commits

Each commit message consists of a **header**, a **body** and a **footer**. The header has a special
format that includes a **type**, a **scope** and a **subject**:

```
<type>(<scope>): <subject>
<BLANK LINE>
<body>
<BLANK LINE>
<footer>
```

For example:

```
feat(client): support http retries

Add support for http retries to the client with
exponentian backoff.

Ref: #123, #456
Fix: #789
```

Or

```
chore(deps): update dependency @types/node to v14.14.31
```

```
refactor: remove unused code
```

```
fix(server): fix crash on startup

Fixes #123
```


## Style guide

We use `github.com/go-faster/errors` for error handling.

See full [Style Guide](https://go-faster.org/docs/style-guide) that is based on [Uber Style Guide](https://github.com/uber-go/guide/blob/master/style.md).
