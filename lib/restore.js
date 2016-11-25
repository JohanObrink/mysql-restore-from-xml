const {createReadStream} = require('fs')
const {parseString} = require('xml2js')
const {resolve} = require('path')

const Parser = require('xml-streamer')

function attrs (node) {
  return node['$']
}

function parseTable (data) {
  const tableName = attrs(data).name

  const fields = []
  const primaries = []
  const uniques = []
  const multiples = []
  
  data.field
    .forEach(field => {
      const rxBlobText = /(?:blob$)|(?:text$)/gi
      const rxString = /(?:char)|(?:text$)/gi
      const attributes = attrs(field)

      const name = '`' + attributes.Field + '`'
      const type = attributes.Type
      const def = (rxBlobText.exec(type)) ? `${name}(255)` : name
      
      // field definition
      {
        let sql = ` ${name} ${type}`
        if (attributes.Null === 'NO') {
          sql += ' NOT NULL'
        }
        if (attributes.Default && attributes.Default !== 'null') {
          const value = rxString.exec(type) ? `'${attributes.Default}'` : attributes.Default
          sql += ` DEFAULT ${value}`
        }
        if (attributes.Extra) {
          sql += ' ' + attributes.Extra
        }
        fields.push(sql)
      }

      // keys / indexes
      if (attributes.Key) {
        switch (attributes.Key) {
          case 'PRI':
            primaries.push({name, def})
            break
          case 'UNI':
            uniques.push({name, def})
            break
          case 'MUL':
            multiples.push({name, def})
            break
        }
      }
    })

  const definition = fields.slice()

  if (primaries.length) {
    definition.push(` PRIMARY KEY (${primaries.map(key => key.def).join(', ')})`)
  }
  uniques.forEach(key => definition.push(` UNIQUE KEY ${key.name} (${key.def})`))
  multiples.forEach(key => definition.push(` UNIQUE KEY ${key.name} (${key.def})`))

  const sql = ['CREATE TABLE IF NOT EXISTS `' + tableName + '` (']
    .concat(definition.join(',\n'))
    .concat(') ENGINE=InnoDB AUTO_INCREMENT=1 ;\n')
    .join('\n')
}

function createParser (name, resourcePath, dataParser) {
  const parser = new Parser({resourcePath})
  parser.on('data', dataParser)

  parser.on('end', () => {
    console.log('end')
  })
  parser.on('error', (error) => {
    console.error('error', error)
  })
  return parser
}

function restore (filepath) {
  const xmlStream = createReadStream(filepath, {encoding: 'utf8'})
  const tableParser = createParser('table', '/mysqldump/database/table_structure', data => parseTable(data))
  // const dataParser = createParser('email_tracking', '/mysqldump/database/table_data/row')

  xmlStream.pipe(tableParser)
  //xmlStream.pipe(dataParser)
}

function run () {
  const path = process.argv[2]
  const fullPath = resolve(path)
  return restore(path)
}

module.exports = run
