# Large JSON Data Support

This document explains the changes made to JSONCrack to support large JSON data without URL limitations.

## Problem

The original JSONCrack implementation passed JSON data through URL parameters, which has limitations:
- **431 Error**: Request Entity Too Large when data exceeds URL length limits
- **Browser limitations**: Most browsers limit URLs to ~8KB
- **Server limitations**: Some servers have URL length restrictions

## Solution

### 1. API Endpoints

Created two new API endpoints to handle large JSON data:

#### `/api/json-data` (POST)
- Receives JSON data via POST request
- Stores data in server memory with session ID
- Returns redirect URL with session ID
- Supports up to 50MB of JSON data

#### `/api/get-json-data` (GET)
- Retrieves stored JSON data by session ID
- Validates session expiration (30 minutes)
- Returns JSON data for rendering

### 2. Modified Components

#### `useFile.ts`
- Added support for session-based data retrieval
- Handles both URL parameters and session IDs
- Maintains backward compatibility

#### `json-generator.js`
- Automatically detects large JSON data (>8KB)
- Uses POST request for large data
- Falls back to URL method for small data
- Provides detailed logging and error handling

### 3. Configuration Changes

#### `next.config.js`
- Removed static export to enable API routes
- Added body parser size limit (50MB)
- Maintains all existing functionality

## Usage

### For Small JSON Data (< 8KB)
```bash
node json-generator.js --input-file small-data.json
```
Uses URL parameters (original method)

### For Large JSON Data (> 8KB)
```bash
node json-generator.js --input-file large-data.json
```
Automatically uses POST request to API endpoint

### Testing Large Data
```bash
node test-large-json.js
```
Generates and tests 1MB JSON data

## Technical Details

### Session Management
- Data stored in server memory (global.jsonSessions)
- Session IDs expire after 30 minutes
- Automatic cleanup of expired sessions
- Unique session IDs with timestamps

### Error Handling
- Graceful fallback to URL method if POST fails
- Detailed error logging
- Session validation and cleanup

### Performance
- No data size limits (up to 50MB configured)
- Efficient session storage
- Minimal memory overhead

## Development Setup

1. **Install dependencies**:
   ```bash
   npm install
   ```

2. **Start development server**:
   ```bash
   npm run dev
   ```

3. **Test with large data**:
   ```bash
   node test-large-json.js
   ```

## Production Considerations

For production deployment, consider:

1. **Session Storage**: Replace in-memory storage with Redis or database
2. **Security**: Add authentication and rate limiting
3. **Monitoring**: Add logging and metrics
4. **Scaling**: Consider load balancing for high traffic

## Backward Compatibility

All existing functionality remains intact:
- URL parameters still work for small data
- Existing JSONCrack features unchanged
- No breaking changes to API

## Troubleshooting

### Common Issues

1. **431 Error Still Occurs**
   - Ensure development server is running
   - Check that API routes are accessible
   - Verify JSON data is valid

2. **Session Not Found**
   - Sessions expire after 30 minutes
   - Check server logs for errors
   - Verify session ID format

3. **Large Data Not Loading**
   - Check browser console for errors
   - Verify API endpoint responses
   - Ensure proper Content-Type headers

### Debug Mode

Enable detailed logging by setting environment variable:
```bash
DEBUG=jsoncrack:* npm run dev
``` 