function transform(line) {

  var values = line.split(",");

  return JSON.stringify({
    id: parseInt(values[0]),
    name: values[1],
    age: parseInt(values[2]),
    department: values[3],
    salary: parseInt(values[4])
  });
}