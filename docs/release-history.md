## 4.14.0

### Bugs Fixed

- Fixed deserializing parquet generated by Azure Data Explorer non-native writer by [@mcbos](https://github.com/mcbos) in #357.

### Release Pipeline

- build: re-worked build pipeline to separate build and release stage.
- release: use handcrafted release notes file and cut out last version notes with `grep`/`head`/`tail` on release. This is in order to improve release notes experience as autogenerated ones are often of sub-par quality.

## 4.13.0

- Add support for deserializing required strings by [@mcbos](https://github.com/mcbos) in [#341](https://github.com/aloneguid/parquet-dotnet/pull/341)
- Add support to .NET 6 TimeOnly type by [@ramon-garcia](https://github.com/ramon-garcia) in [#352](https://github.com/aloneguid/parquet-dotnet/pull/352)
- Support for reading and writing column chunk metadata by [@aloneguid](https://github.com/aloneguid) in [#354](https://github.com/aloneguid/parquet-dotnet/pull/354)