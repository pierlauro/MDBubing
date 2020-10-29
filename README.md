## MDBubing: from WARC records to MongoDB documents

![unit-tests+lint](https://github.com/pierlauro/MDBubing/workflows/unit-tests+linting/badge.svg)
![integration-tests](https://github.com/pierlauro/MDBubing/workflows/integration-tests/badge.svg)

**MDBubing** - ridiculous wordplay to merge the words MongoDB and [BUbiNG](https://github.com/LAW-Unimi/BUbiNG) - is a library aimed to:
- Make it simple to migrate existing WARC files into MongoDB, namely exporting each record in a separate document.
- Save MongoDB documents at BUbiNG crawl-time, bypassing WARC files creation.

### Usage

#### Migrate WARC records to MongoDB documents
1) Create a properties file defining the values of the following fields:
- *connectionString*: the [connection string](https://docs.mongodb.com/manual/reference/connection-string/) of a MongoDB instance (you can also use it to specify eventual write majority concerns)
- *database*: the database
- *collection*: the collection to save documents in
- *warcFilePath*: path of a WARC file (formats supported: `.warc` and `.warc.gz`)

You can refer to the following sample configuration: [WarcToMongo-sample-configuration.properties](https://github.com/pierlauro/MDBubing/blob/master/src/test/resources/WarcToMongo-sample-configuration.properties).

2) Execute the following command:
```
$ java dev.pstux.mdbubing.WarcToMongo -P <properties_file_path>
```
3) Wait for the records to be exported and enjoy!

#### Save Mongo documents at crawl time
TODO: document this section


### Development

This project expects source files to be formatted following the [Google Java style](https://google.github.io/styleguide/javaguide.html). Run `./gradlew goJF` in order to automatically format all `.java` files under `src/`.

Execute unit tests (mocking MongoDB entities): `./gradlew test`.

Execute integration tests (automatically running a MongoDB docker container, testing, and shutting the instance down): `./gradlew integrationTestWithDocker`.

CI tasks definitions can be found in the [workflows directory](https://github.com/pierlauro/MDBubing/tree/master/.github/workflows).


#### TODO
- Javadoc
- Upload artifact to sonatype
- Add ability to write into multiple collections
- Map by default `WARC-Record-ID` into `_id` field
- Add ability to specify the desired `<WARC header, document field>` mapping
- Performance benchmarks
