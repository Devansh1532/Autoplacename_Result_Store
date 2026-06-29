import { z } from "zod";

// const regex = /<script\b[^>]*>/i;

const externalSchema = z.object({
  // placeId: z.string().transform((val)=> regex.test(val)),
  placeId: z.string(),
  timestamp: z.string(),
  lat: z.number().transform((val) => val.toFixed(4)),
  lng: z.number().transform((val) => val.toFixed(4)),
  postcode: z.string(),
  address: z.string(),
  source: z.string(),
});

const clickSchema = z.object({
  searchQuery: z.string(),
  timestamp: z.string(),
  placeId: z.string(),
});

const orsSchema = z.object({
  from_lat: z.number().transform((val) => val.toFixed(4)),
  from_lng: z.number().transform((val) => val.toFixed(4)),
  to_lat: z.number().transform((val) => val.toFixed(4)),
  to_lng: z.number().transform((val) => val.toFixed(4)),
  timestamp: z.string()
})


export {externalSchema, clickSchema, orsSchema}
