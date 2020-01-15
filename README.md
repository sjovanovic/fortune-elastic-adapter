# Fortune ElasticSearch adapter

This is an ElasticSearch adapter for Fortune.js. 

## Features

- uses official elasticsearch client
- `Buffer` fields are saved as non indexed arrays in ElasticSearch


```sh
$ npm install fortune-elastic-adapter
```


## Usage

This adapter works in Node.js only:

```js
const fortune = require('fortune')
const elasticAdapter = require('fortune-elastic-adapter')

const store = fortune(recordTypes, {
  adapter: [ elasticAdapter, {
    /**
     * ElasticSearch adapter. Available options:
     *
     * - `hosts`:       ElasticSearch hosts array. Default: `["http://localhost:9200"]`.
     * - `index`:       Name of the ElasticSearch index. Default: `fortune`.
     * - `log`:         Log level. One of: `debug`, `trace`, `error`, `warning`. Default: `error`.
     * - `apiVersion`:  ElasticSearch API version. Default: `2.4`.
     */
    hosts: ["http://localhost:9200"]
  } ]
})
```

For full list of options read [the official elasticsearch client configuration options](https://www.elastic.co/guide/en/elasticsearch/client/javascript-api/current/client-configuration.html)


## License

This software is licensed under the [MIT license](https://raw.githubusercontent.com/fortunejs/fortune-indexeddb/master/LICENSE).