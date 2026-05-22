import mongoose from "mongoose";


const externalDataSchema = new mongoose.Schema({
  placeId: {
    type: String,
    required: false,
    unique: false,
  },
  timestamp: {
    type: String,
    required: false,
    unique: false,
  },
  placenameEN: {
    type: String,
    required: false,
    unique: false,
  },

  placenameAR: {
    type: String,
    required: false,
    unique: false,
  },

  placenameES: {
    type: String,
    required: false,
    unique: false,
  },

  lat: {
    type: Number,
    required: false,
    unique: false,
  },

  lng: {
    type: Number,
    required: false,
    unique: false,
  },

  type: {
    type: String,
    required: false,
    unique: false,
  },

  iata: {
    type: String,
    required: false,
    unique: false,
  },

  postcode: {
    type: String,
    required: false,
    unique: false,
  },

  address: {
    type: String,
    required: false,
    unique: false,
  },

  source: {
    type: String,
    required: false,
    unique: false,
  },
});

const clickResultSchema = new mongoose.Schema({
  searchQuery: {
    type: String,
    required: false,
    unique: false,
  },
  
  timestamp: {
    type: String,
    required: false,
    unique: false,
  },
  placeId: {
    type: String,
    required: false,
    unique: false,
  },
  source: {
     type: String,
     required: false
  },
  insertedAt: {
    type: String,
    required: false
  }
  
});

const placeCountSchema = new mongoose.Schema({
  placeId: {
    type: String,
    required: false,
    unique: false,
  },

  count: {
    type: Number,
    required: false,
    unique: false,
  },

  timestamp: {
    type: String,
    required: false,
    unique: false,
  },
});

const routeCountSchema = new mongoose.Schema({
  route_id: {
    type: String,
    required: true,
    unique: false,
  },

  count: {
    type: Number,
    required: false,
    unique: false,
  },

  timestamp: {
    type: String,
    required: false,
    unique: false,
  },
});

const orsDataSchema = new mongoose.Schema({
  route_id: {
    type: String,
    required:false,
    unique:false
  },
  from_lat: {
    type: Number,
    required: false,
    unique: false,
  },
  from_lng: {
    type: Number,
    required: false,
    unique: false,
  },
  to_lat: {
    type: Number,
    required: false,
    unique: false,
  },

  to_lng: {
    type: Number,
    required: false,
    unique: false,
  },

  distance_km: {
    type: Number,
    required: false,
    unique: false,
  },

  duration_min: {
    type: Number,
    required: false,
    unique: false,
  },
  source: {
    type: String,
    required: false,
    unique: false
  }
});

const externalData = mongoose.model("backup_data", externalDataSchema);
const clickValue = mongoose.model("click_result", clickResultSchema);
const placeCountValue = mongoose.model("place_count_value", placeCountSchema);
const routeCountValue = mongoose.model("route_count_value", routeCountSchema)
const orsData = mongoose.model("ors_response", orsDataSchema);

export { externalData, clickValue, placeCountValue, routeCountValue, orsData };


