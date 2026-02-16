const mongoose = require('mongoose')


mongoose.set("strictQuery", true)
async function connectMongoDB(url){
    try {
      const result = await mongoose.connect(url)
      return result
  } catch (err) {
      return err
  }
}


module.exports = {connectMongoDB}