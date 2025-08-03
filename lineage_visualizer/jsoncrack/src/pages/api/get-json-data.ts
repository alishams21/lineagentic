import { NextApiRequest, NextApiResponse } from 'next';

export default async function handler(req: NextApiRequest, res: NextApiResponse) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  try {
    const { sessionId } = req.query;

    if (!sessionId || typeof sessionId !== 'string') {
      return res.status(400).json({ error: 'Session ID is required' });
    }

    // Check if global sessions exist
    if (!global.jsonSessions) {
      return res.status(404).json({ error: 'No sessions available' });
    }

    const session = global.jsonSessions.get(sessionId);

    if (!session) {
      return res.status(404).json({ error: 'Session not found' });
    }

    // Check if session has expired
    if (session.expiresAt < Date.now()) {
      global.jsonSessions.delete(sessionId);
      return res.status(410).json({ error: 'Session expired' });
    }

    // Return the JSON data
    res.status(200).json({
      success: true,
      data: session.data,
      timestamp: session.timestamp,
      expiresAt: session.expiresAt
    });

  } catch (error) {
    console.error('Error retrieving JSON data:', error);
    res.status(500).json({ error: 'Internal server error' });
  }
} 