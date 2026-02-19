const mongoose = require('mongoose')

const backupDataSchema = new mongoose.Schema({
    
    searchQuery: {
        type: String,
        required: false,
        unique: false
    },
    
    timestamp: {
        type: String,
        required: false,
        unique: false
    },
    place : {
        type: Map,
        of: mongoose.Schema.Types.Mixed,
        required: false,
        unique: false
    },
    source : {
        type: String,
        required: false,
        unique:false

    }
    
})


const clickResultSchema = new mongoose.Schema({
    
    searchQuery: {
        type: String,
        required: false,
        unique: false
    },
    
    timestamp: {
        type: String,
        required: false,
        unique: false
    },
    placeId : {
        type: String,
        required: false,
        unique: false
    }

    }
    
)

const countSchema = new mongoose.Schema({
    
    placeId : {
        type: String,
        required: false,
        unique: false
    },

    count : {
        type: Number,
        required: false,
        unique: false
    },

    timestamp: {
        type: String,
        required: false,
        unique: false
    }
    

    }
    
)

const backupData = mongoose.model("backup_data", backupDataSchema)
const clickValue = mongoose.model("click_result", clickResultSchema)
const countValue = mongoose.model("count_value", countSchema)

module.exports = {backupData, clickValue, countValue}