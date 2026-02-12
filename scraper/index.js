#!/usr/bin/env node

const { spawn } = require('child_process');
const path = require('path');

// Get the Python executable from the venv
const pythonExe = path.join(__dirname, '..', '.venv', 'bin', 'python');
const scriptPath = path.join(__dirname, 'src', 'main.py');

// Spawn the Python process and pipe output directly
const pythonProcess = spawn(pythonExe, [scriptPath], {
  stdio: 'inherit',
  cwd: path.join(__dirname, '..')
});

// Exit with the same code as the Python process
pythonProcess.on('exit', (code) => {
  process.exit(code);
});
