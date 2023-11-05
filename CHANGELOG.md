# Changelog

## [0.2.1](https://github.com/tsanton/datasourcerer/compare/0.2.0...0.2.1) (2023-11-05)


### Bug Fixes

* added postgres parser to main ([38fc053](https://github.com/tsanton/datasourcerer/commit/38fc05383586c75666278ed6fc3f587882875733))

## [0.2.0](https://github.com/tsanton/datasourcerer/compare/0.1.0...0.2.0) (2023-11-05)


### ⚠ BREAKING CHANGES

* postgres csv finished with time and timestamp tz and ntz types
* simplify snowflake timestamp with datetime, timestamp_tz, timestamp_ltz and timestamp_ntz

### Features

* added postgres csv bigint parser ([3a24e24](https://github.com/tsanton/datasourcerer/commit/3a24e24cbf52ccafa8465f482d67f3543c35227a))
* added postgres csv boolean parser ([21d6dd2](https://github.com/tsanton/datasourcerer/commit/21d6dd2bbb7735376aa3aa1a263dd7ebfa6d671e))
* added postgres csv date parser ([46efd96](https://github.com/tsanton/datasourcerer/commit/46efd96961c2ce7ddf19d20bed8c7de84f80adf8))
* added postgres csv jsonb parser ([36e0e9c](https://github.com/tsanton/datasourcerer/commit/36e0e9cbc8a6adc8bdca97efd6972dbdea2128f1))
* added postgres csv numeric parser ([576afbd](https://github.com/tsanton/datasourcerer/commit/576afbd267410c37ab08edcf304098b69eb1d0f8))
* added postgres csv smallint parser ([5da9432](https://github.com/tsanton/datasourcerer/commit/5da94322ce2fad8ba119c39003ed1e32c0eb571d))
* added postgres csv text parser ([fa5ca45](https://github.com/tsanton/datasourcerer/commit/fa5ca4588abe5a8e215b60e2bccdf0c49017c282))
* added postgres csv time no time zone parser ([6183f1a](https://github.com/tsanton/datasourcerer/commit/6183f1ade9adac572fdff08e9d5a54dfe7c0434a))
* added postgres csv time with time zone parser ([640ec2c](https://github.com/tsanton/datasourcerer/commit/640ec2c2a57b77fa07d8776a60b7a865bd9ae2af))
* added postgres csv timestamp no time zone parser ([8f8d98f](https://github.com/tsanton/datasourcerer/commit/8f8d98ff115512646b2bf82115bbce5f7e8feb3b))
* added postgres csv timestamp with time zone parser ([a3d8ea2](https://github.com/tsanton/datasourcerer/commit/a3d8ea2706bc224e85b667d20901223115f90cf6))
* added postgres csv- and sqlreader to formatter ([2f0e167](https://github.com/tsanton/datasourcerer/commit/2f0e1673a0c6e2f8c41dfe80f643d988e1f76a78))
* added postgres csvreader ([467f0cb](https://github.com/tsanton/datasourcerer/commit/467f0cbc8c8c5dac0d09c6294e69ea7a60f097c2))
* added postgres formatter and sqlwriter ([da60cb9](https://github.com/tsanton/datasourcerer/commit/da60cb99274c9d3a322f527ea0fe69bf1ba86975))
* added postgres sqlreader ([85a2bdc](https://github.com/tsanton/datasourcerer/commit/85a2bdca1a9917e14cba08107c995c07cd6bf5f1))
* postgres csv finished with time and timestamp tz and ntz types ([647d301](https://github.com/tsanton/datasourcerer/commit/647d301c3c9ff318095cfefe135d53110181fd55))
* postgres csv int in range validation ([aa5aa98](https://github.com/tsanton/datasourcerer/commit/aa5aa989cf0781315416a51ef3cedecd754fab67))


### Bug Fixes

* ensuring and testing for case insensitivity ([69aa786](https://github.com/tsanton/datasourcerer/commit/69aa786a3cfb6a44bd70c14036b52dc9eb0aa8c1))


### Code Refactoring

* simplify snowflake timestamp with datetime, timestamp_tz, timestamp_ltz and timestamp_ntz ([d8db6cd](https://github.com/tsanton/datasourcerer/commit/d8db6cd2dcb65b56078469d24fc088b762793296))

## [0.1.0](https://github.com/tsanton/datasourcerer/compare/v0.1.0...0.1.0) (2023-10-31)


### Features

* init commit ([efe3d0c](https://github.com/tsanton/datasourcerer/commit/efe3d0c458e813c8800362cb90931dff8c3e9df8))
