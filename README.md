# Project Nessie

[![Build Status](https://github.com/projectnessie/nessie/workflows/Main%20CI/badge.svg)](https://github.com/projectnessie/nessie/actions)
[![codecov](https://codecov.io/gh/projectnessie/nessie/branch/main/graph/badge.svg?token=W9J9ZUYO1Y)](https://codecov.io/gh/projectnessie/nessie)
[![Maven Central](https://img.shields.io/maven-central/v/org.projectnessie/nessie)](https://search.maven.org/artifact/org.projectnessie/nessie)
[![PyPI](https://img.shields.io/pypi/v/pynessie.svg)](https://pypi.python.org/pypi/pynessie)
[![Docker](https://img.shields.io/docker/v/projectnessie/nessie/latest?label=docker)](https://hub.docker.com/r/projectnessie/nessie)

Project Nessie is a system to provide Git like capability for Iceberg Tables, Delta Lake Tables and Sql Views.

More information can be found at [projectnessie.org](https://projectnessie.org/).


## Using Nessie

You can quickly get started with Nessie by using our small, fast docker image.

```
docker pull projectnessie/nessie
docker run -p 19120:19120 projectnessie/nessie
```

A local [Web UI](https://projectnessie.org/tools/ui/) will be available at this point.

Then install the Nessie CLI tool (to learn more about CLI tool and how to use it, check [here](https://projectnessie.org/tools/cli/)

```
pip install pynessie
```

From there, you can use one of our technology integrations such those for 

* [Spark via Iceberg](https://projectnessie.org/tools/iceberg/spark/)
* [Hive via Iceberg](https://projectnessie.org/tools/iceberg/hive/)
* [Spark via Delta Lake](https://projectnessie.org/tools/deltalake/spark/)

To learn more about all supported integrations and tools, check [here](https://projectnessie.org/tools/) 

Have fun! We have a Google Group and a Slack channel we use for both developers and 
users. Check them out [here](https://projectnessie.org/community/).

### Authentication

By default, Nessie servers run with authentication disabled and all requests are processed under the "anonymous"
user identity.

Nessie supports bearer tokens and uses [OpenID Connect](https://openid.net/connect/) for validating them.

Authentication can be enabled by setting the following Quarkus properties:
* `nessie.server.authentication.enabled=true`
* `quarkus.oidc.auth-server-url=<OpenID Server URL>`
* `quarkus.oidc.client-id=<Client ID>`

#### Experimenting with Nessie Authentication in Docker

One can start the `projectnessie/nessie` docker image in authenticated mode by setting
the properties mentioned above via docker environment variables. For example:

```
docker run -p 19120:19120 -e QUARKUS_OIDC_CLIENT_ID=<Client ID> -e QUARKUS_OIDC_AUTH_SERVER_URL=<OpenID Server URL> -e NESSIE_SERVER_AUTHENTICATION_ENABLED=true --network host projectnessie/nessie
```

## Building and Developing Nessie

### Requirements

- JDK 11 or higher: JDK11 or higher is needed to build Nessie (artifacts are built 
  for Java 8)

### Installation

Clone this repository and run maven:
```bash
git clone https://github.com/projectnessie/nessie
cd nessie
./mvnw clean install
```

#### Delta Lake artifacts

Nessie required some minor changes to Delta for full support of branching and history. These changes are currently being integrated into the [mainline repo](https://github.com/delta-io/delta). Until these have been merged we have provided custom builds in [our fork](https://github.com/projectnessie/delta) which can be downloaded from a separate maven repository. 

### Distribution
To run:
1. configuration in `servers/quarkus-server/src/main/resources/application.properties`
2. execute `./mvnw quarkus:dev`
3. go to `http://localhost:19120`

### UI 
To run the ui (from `ui` directory):
1. If you are running in test ensure that `setupProxy.js` points to the correct api instance. This ensures we avoid CORS
issues in testing
2. `npm install` will install dependencies
3. `npm run start` to start the ui in development mode via node

To deploy the ui (from `ui` directory):
1. `npm install` will install dependencies
2. `npm build` will minify and collect the package for deployment in `build`
3. the `build` directory can be deployed to any static hosting environment or run locally as `serve -s build`

### Docker image

When running `mvn clean install -Pdocker` a docker image will be created at `projectnessie/nessie` which can be started 
with `docker run -p 19120:19120 projectnessie/nessie` and the relevant environment variables. Environment variables
are specified as per https://github.com/eclipse/microprofile-config/blob/master/spec/src/main/asciidoc/configsources.asciidoc#default-configsources  


### AWS Lambda
You can also deploy to AWS lambda function by following the steps in `servers/lambda/README.md`
 

## Contributing

### Code Style

The Nessie project uses the Google Java Code Style, scalafmt and pep8.
See [CONTRIBUTING.md](./CONTRIBUTING.md) for more information.
