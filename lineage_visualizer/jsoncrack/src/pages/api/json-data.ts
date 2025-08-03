import { NextApiRequest, NextApiResponse } from 'next';

// Configure body parser for large JSON data
export const config = {
  api: {
    bodyParser: {
      sizeLimit: '50mb',
    },
  },
};

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'POST') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { jsonData } = req.body;

    if (!jsonData) {
      return res.status(400).json({ error: 'No JSON data provided' });
    }

    // Validate that it's valid JSON
    let parsedData;
    try {
      parsedData = typeof jsonData === 'string' ? JSON.parse(jsonData) : jsonData;
    } catch (error) {
      return res.status(400).json({ error: 'Invalid JSON data' });
    }

    // Store the data in a session or temporary storage
    // For now, we'll use a simple in-memory storage (in production, use Redis or similar)
    const sessionId = `json_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
    
    // Store in global variable (in production, use proper session management)
    if (!global.jsonSessions) {
      global.jsonSessions = new Map();
    }
    global.jsonSessions.set(sessionId, {
      data: parsedData,
      timestamp: Date.now(),
      expiresAt: Date.now() + (30 * 60 * 1000) // 30 minutes
    });

    // Clean up expired sessions
    for (const [key, session] of global.jsonSessions.entries()) {
      if (session.expiresAt < Date.now()) {
        global.jsonSessions.delete(key);
      }
    }

    // Return the session ID and redirect URL
    const origin = req.headers.origin || 'http://localhost:3000';
    const redirectUrl = `${origin}/editor?session=${sessionId}`;
    
    res.status(200).json({
      success: true,
      sessionId,
      redirectUrl,
      dataSize: JSON.stringify(parsedData).length
    });

  } catch (error) {
    console.error('Error processing JSON data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
} 