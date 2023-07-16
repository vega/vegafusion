# Pin third-part actions
For improved security, third-party GitHub actions are pinned using [pin-github-action](https://github.com/mheap/pin-github-action). To add pins:

```
GH_ADMIN_TOKEN={token} pin-github-action .github/workflows/build.yml
```
