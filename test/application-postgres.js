
// create a pipeline that reads documents from a file, transforms them, and writes them
Source({name:"postgres", namespace:"public\..*"}).save({name:"loosefile", uri:"file:///tmp/foo"})
