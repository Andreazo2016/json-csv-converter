const { pipeline, Readable, Transform } = require('stream')
const { promisify } = require('util')
const { createWriteStream } = require('fs')
const people = require('./resources/fake_people.json')

const pipelineAsync = promisify(pipeline)

const readJob = Readable({
  read() {
    const context = this
    people.forEach((person, index) => {
      context.push(JSON.stringify(person))
    })
    context.push(null)
  }
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
  console.log('total itens: ', people.length)
  console.time('time')
  await pipelineAsync(readJob, mapToCSV, setHeader, createWriteStream('./resources/people.csv'))
  console.timeEnd('time')
}
main()