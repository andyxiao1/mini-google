import React from 'react'
import { createMuiTheme, ThemeProvider} from '@material-ui/core'
import { Dashboard } from '../pages/Dashboard'

const darkTheme = createMuiTheme({
  palette: {
    type: 'dark',
  },
});

export default function App() {
  return (
    <ThemeProvider theme={darkTheme}> 
      <Dashboard />
    </ThemeProvider>
  )
}

