const { z } = require('zod');

const NormalizedEventSchema = z.object({
  eventId: z.string().uuid(),
  source: z.enum(['website', 'mobile_app', 'call_center', 'in_person']),
  timestamp: z.string().datetime(),
  
  // Expanded Identity Payload
  identifiers: z.object({
    // Hard Identifiers
    email: z.string().email().optional(),
    phone: z.string().optional(),
    registeredUserId: z.string().optional(),
    accountId: z.string().optional(),
    
    // Soft Identifiers
    deviceId: z.string().optional(),
    cookieId: z.string().optional(),
    ipAddress: z.string().optional(), 
    userAgent: z.string().optional(),
    location: z.string().optional(), 
  }).refine((data) => Object.keys(data).length > 0, {
    message: "Event must contain at least one valid identifier for resolution.",
  }),

  action: z.string(), 
  payload: z.record(z.any()).optional(), 
});

module.exports = { NormalizedEventSchema };