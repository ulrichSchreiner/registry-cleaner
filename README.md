# docker registry cleaner

Cleans a docker registry and removes images older than n days.

Call with
```
registry-cleaner -user <userid> -password <password> -num <days-to-keep> <url-of-registry>
```