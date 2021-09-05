const { createWriteStream, createReadStream } = require('fs')
const { pipeline, Readable, Transform } = require('stream')
const { promisify } = require('util')

const JSONStream = require('JSONStream');

const stream = createReadStream('./resources/fake_people.json')

const parser = JSONStream.parse("*");
stream.pipe(parser);

const pipelineAsync = promisify(pipeline)

const readJob = Readable({
  read() { }
})

parser.on('data', function (obj) {
  readJob.push(JSON.stringify(obj))
})
parser.on('end', function () {
  readJob.push(null)
  console.log('Terminou!!')
})


const mapToCSV = Transform({
  transform(chunk, enconding, cb) {
    const person = JSON.parse(chunk)
    const line = `${person.name},${person.email},${person.gender}\n`
    cb(null, line)
  }
})

const setHeader = Transform({
  transform(chunk, enconding, cb) {
    this.counter = this.counter ?? 0
    if (this.counter) {
      return cb(null, chunk)
    }
    this.counter += 1
    cb(null, "name,email,gender\n".concat(chunk))
  }
})


async function main() {
  console.time('time')
  await pipelineAsync(readJob, mapToCSV, setHeader, createWriteStream('./resources/people.csv'))
  console.timeEnd('time')
}
main()

