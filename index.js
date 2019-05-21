'use strict'

const elasticsearch = require('elasticsearch')

/**
 * ElasticSearch adapter. Available options:
 *
 * - `hosts`:       ElasticSearch hosts array. Default: `["http://localhost:9200"]`.
 * - `index`:       Name of the ElasticSearch index. Default: `fortune`.
 * - `log`:         Log level. One of: `debug`, `trace`, `error`, `warning`. Default: `error`.
 * - `apiVersion`:  ElasticSearch API version. Default: `2.4`.
 */
module.exports = function (Adapter) {
  var DefaultAdapter = Adapter.DefaultAdapter
  var map, primaryKey

  function ElasticAdapter (properties) {
    DefaultAdapter.call(this, properties)

    if (!this.options.hosts)
      this.options.hosts = ["http://localhost:9200"]

    if (!this.options.index)
      this.options.index = 'fortune'

    if (!this.options.log)
      this.options.log = 'error'
    
    if (!this.options.apiVersion)
      this.options.apiVersion = '2.4'

    // No LRU, allow as many records as possible.
    // delete this.options.recordsPerType

    primaryKey = properties.common.constants.primary
    map = properties.common.map
  }

  ElasticAdapter.features = {
    logicalOperators: false // whether or not and and or queries are supported.
  }

  ElasticAdapter.prototype = Object.create(DefaultAdapter.prototype)


  ElasticAdapter.prototype.connect = function () {
    var self = this

    return DefaultAdapter.prototype.connect.call(self).then(function () {




        return new Promise(function (resolve, reject) {

            try{

                let supported = ['hosts', 'log', 'apiVersion']
                let opts = {}
                for(var i in self.options){
                    if(supported.indexOf(i) != -1){
                        opts[i] = self.options[i]
                    }
                }
                self.ES = new elasticsearch.Client(opts);

                // makes ure the database mappings are okay with these record types
                return self.ES.indices.create({index:self.options.index})
                    .then(()=>mappingConsistency(self.options.index, self.recordTypes, self.ES).then(()=>resolve(self.ES)))
                    .catch(()=>mappingConsistency(self.options.index, self.recordTypes, self.ES).then(()=>resolve(self.ES)))
                    
            }catch(err){
                reject(err)
            }
        })
      }).catch((err)=>{throw err})
  }


  ElasticAdapter.prototype.disconnect = function () {
    return DefaultAdapter.prototype.disconnect.call(this)
  }


  ElasticAdapter.prototype.create = function (type, records) {
    var self = this

    // IMPORTANT: the record must have initial values for each field defined in the record type. For non-array fields, it should be null, and for array fields it should be [] (empty array). Note that not all fields in the record type may be enumerable, such as denormalized inverse fields, so it may be necessary to iterate over fields using Object.getOwnPropertyNames.
    return DefaultAdapter.prototype.create.call(self, type, records)
      .then(function (records) {


        let bulk = []
        records.forEach((rec)=>{
            bulk.push({
                create: {
                    _index: self.options.index,
                    _type: type,
                    _id: rec.id
                }
            })
            bulk.push(rec)
        })

        return self.ES.bulk({
            body: bulk
        }).then((resp)=>{
            return Promise.resolve(records)
        })
      })
  }

//   {
//     sort: { age: false, // descending, name: true // ascending },
//     fields: { foo: true, bar: true },
//     exists: { name: true, // check if this fields exists, age: false // check if this field doesn't exist },
//     match: { 
//      name: 'value', // exact match or containment if array
//      friends: [ 'joe', 'bob' ] // match any one of these values
//     },
//     range: { 
        //     age: [ 18, null ], // From 18 and above.
        //     name: [ 'a', 'd' ], // Starting with letters A through C.
        //     createdAt: [ null, new Date(2016, 0) ] // Dates until 2016.
        // },
        
//     // Limit results to this number. Zero means no limit.
//     limit: 0,
  
//     // Offset results by this much from the beginning.
//     offset: 0,
  
//     // The logical operator "and", may be nested. Optional feature.
//     and: { ... },
  
//     // The logical operator "or", may be nested. Optional feature.
//     or: { ... },
  
//     // Reserved field for custom querying.
//     query: null
//   }
// The return value of the promise should be an array, 
// and the array MUST have a count property that is the total number of records without limit and offset.



  ElasticAdapter.prototype.find = function (type, ids, options) {
    var self = this

    options = options || {}

    let search = {
        "query": {
            "bool": {
                "filter":[],
                "must": [],
                "must_not":[],
                "should":[]
            }
        }
    }

    // size
    if(options.limit > 0){
        search.size = options.limit
    }else if(options.limit === 0){
        search.size = 100
    }

    // from
    if(options.offset && !isNaN(options.offset)){
        search.from = options.offset
    }

    // sort 
    if(options.sort){
        search.sort = []
        for(var i in options.sort){
            let srt = {}
            srt[i] = options.sort[i] === true ? {"order" : "asc"} : {"order" : "desc"}
            search.sort.push(srt)
        }
        search.sort.push("_doc")
    }

    // includes / excludes
    if(options.fields){
        search._source = {
            "includes": [],
            "excludes": []
        }

        for(var i in options.fields){
            if(options.fields[i] === true){
                search._source.includes.push(i)
            }else{
                search._source.excludes.push(i)
            }
        }

        // always include id
        if(search._source.includes.length && search._source.includes.indexOf('id') == -1){
            search._source.includes.push('id')
        }else if(!search._source.includes.length){
            delete search._source.includes
        }
        //search._source = true
    }

    // exists 
    if(options.exists){
        for(var i in options.exists){
            if(options.exists[i] === true){
                search.query.bool.must.push({
                    "exists" : { "field" : i }
                })
            }else{
                search.query.bool.must_not.push({
                    "exists" : { "field" : i }
                })
            }
        }
    }

    // (match)
    if(options.match){
        for(var i in options.match){
            if(Array.isArray(options.match[i])){
                options.match[i].forEach((val)=>{
                    var mtc = { "match_phrase" : {} }
                    mtc.match_phrase[i] = val
                    search.query.bool.must.push(mtc)
                })
            }else{
                var mtc = { "match_phrase" : {} }
                mtc.match_phrase[i] = options.match[i]
                search.query.bool.must.push(mtc)
            }
        }
    }

    // range 
    if(options.range){
        for(var i in options.range){
            let mtc = {"range":{}}
            mtc.range[i] = {}

            let isRange = true
            if(options.range[i][0] instanceof Date){
                mtc.range[i].gte = options.range[i][0].toISOString()
            }else if(!isNaN(options.range[i][0])){
                mtc.range[i].gte = options.range[i][0]
            }else if(options.range[i][0] === null){
            }else{
                isRange = false
            }
            if(options.range[i][1] instanceof Date){
                mtc.range[i].lte = options.range[i][1].toISOString()
            }else if(!isNaN(options.range[i][1])){
                mtc.range[i].lte = options.range[i][1]
            }else if(options.range[i][1] === null){
            }else{
                isRange = false
            }

            if(isRange) search.query.bool.filter.push(mtc)
        }
    }

    // query 
    if(options.query){
        search.query.bool.must.push({
            "query_string": {
                "query": options.query
            }
        })
    }

    // ids
    if(ids && ids.length){
        search.query.bool.must.push({
            "ids" : {
                "values" : ids
            }
        })
    }

    // match all?
    if(!search.query.bool.filter.length && !search.query.bool.must.length && !search.query.bool.must_not.length && !search.query.bool.should.length){
        search.query.bool.must.push({
            "match_all": {}
        })
    }

    //console.log('FIND:', self.options.index+'/'+type, JSON.stringify(search, null, 2))

    return self.ES.search({
        index: self.options.index,
        type: type,
        body: search
    }).then((resp)=>{
        resp.hits.hits.count = resp.hits.total


        let results = []
        results.count = resp.hits.total
        resp.hits.hits.forEach((hit)=>{

            // handle buffers
            for(var i in hit._source){
                if(hit._source[i] && hit._source[i].type == 'Buffer' ){
                    hit._source[i] = Buffer.from(hit._source[i].data)
                }
            }

            results.push(hit._source)
        })

        return Promise.resolve(results)
    }).catch((err)=>{
        console.log('ELASTIC ERROR', err)
        //return Promise.resolve([])
        return Promise.reject(err)
    })
  }


  ElasticAdapter.prototype.update = function (type, updates) {
    var self = this

    /*
    {
        // ID to update. Required.
        id: 1,
      
        // Replace a value of a field. Use a `null` value to unset a field.
        replace: { name: 'Bob' },
      
        // Append values to an array field. If the value is an array, all of
        // the values should be pushed.
        push: { pets: 1 },
      
        // Remove values from an array field. If the value is an array, all of
        // the values should be removed.
        pull: { friends: [ 2, 3 ] },
      
        // The `operate` field is specific to the adapter. This should take
        // precedence over all of the above. Warning: using this may bypass
        // field definitions and referential integrity. Use at your own risk.
        operate: null
    }
    */


    // fetch all first 
    let mget = []
    updates.forEach((update)=>{
        
        let fields = []
        if(update.push){
            for(var i in update.push){
                fields.push(i)
            }
        }
        if(update.pull){
            for(var i in update.pull){
                if(fields.indexOf(i) == -1) fields.push(i)
            }
        }

        mget.push({
            _id: update.id, 
            _type: type,
            _index: self.options.index,
            _source: fields.length ? fields : false
        })
    })


    return self.ES.mget({ body: { docs: mget }}).then((resp) => {
        let docs = {}
        for(var i in resp.docs){
            docs[resp.docs[i]._id] = resp.docs[i]
        }

        let bulk = []
        updates.forEach((update)=>{
            bulk.push({
                update: {
                    _index: self.options.index,
                    _type: type,
                    _id: update.id
                }
            })

            let toUpdate = { ...update.replace }
            if(update.push){
                let doc = docs[update.id]._source
                for(var i in update.push){
                    if(Array.isArray(doc[i])){
                        toUpdate[i] = doc[i]
                        toUpdate[i].push(update.push[i])
                    }else{
                        toUpdate[i] = [doc[i], update.push[i]]
                    }
                }
            }
            if(update.pull){
                let doc = docs[update.id]._source
                for(var i in update.pull){
                    toUpdate[i] = doc[i]
                    if(Array.isArray(update.pull[i])){
                        update.pull[i].forEach( (val)=>toUpdate[i].splice(toUpdate[i].indexOf(val)) )
                    }else{
                        toUpdate[i].splice(toUpdate[i].indexOf(update.pull[i]))
                    }
                }
            }

            if(update.operate && typeof update.operate == 'object'){
                for(var i in update.operate){
                    toUpdate[i] = update.operate[i]
                }
            }

            bulk.push({"doc":toUpdate})
        })

        return self.ES.bulk({
            body: bulk
        }).then((resp) => {
            let count = 0
            resp.items.forEach((item)=>{
                if(item.update.status == 200){
                    count += 1
                }
            })
            return count
        })
    })
  }


  ElasticAdapter.prototype.delete = function (type, ids) {
    var self = this


    // Delete records by IDs, or delete the entire collection if IDs are undefined or empty. Success should resolve to the number of records deleted.
    let bulk = []
    ids.forEach((id)=>{
        bulk.push({
            delete: {
                _index: self.options.index,
                _type: type,
                _id: id
            }
        })
    })

    return self.ES.bulk({
        body: bulk
    }).then((resp)=>{
        let count = 0
        resp.items.forEach((item)=>{
            if(item.delete.status == 200){
                count += 1
            }
        })
        return count
    })
  }

  return ElasticAdapter

  function mappingConsistency(index, recordTypes, client) {
        // make updates
        let promises = []
        let promiseData = []
        let typesToMap = []
        let mapping = {
            "properties": {
                "data": {
                    "type": "long",
                    "index": "no"
                }
            }
        }
        for(var i in recordTypes){
            for(var j in recordTypes[i]){
                if(recordTypes[i][j].type && recordTypes[i][j].type.name == 'Buffer'){
                    let tidx = typesToMap.indexOf(i)
                    if(tidx == -1){
                        let data = {type:i}
                        data[i] = {"properties":{}}
                        data[i].properties[j] = mapping
                        promiseData.push(data)
                        typesToMap.push(i)
                    }else{
                        let data = promiseData[tidx]
                        data[i].properties[j] = mapping
                    }
                }
            }
        }

        // apply updates
        let pnum = promiseData.length
        if(!pnum) return cb()
        for(var i=0; i<pnum; i++){
            promises.push(new Promise((resolve, reject)=>{
                let body = promiseData.pop()
                let type = body.type; delete body.type;
                client.indices.putMapping({index:index, type:type, body:body}, (err, resp)=>{
                    if(err) return reject(err)
                    resolve(resp)
                })
            }))
        }
        return Promise.all(promises)
    }
}