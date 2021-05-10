import React, { useState } from 'react';
import { createMuiTheme, ThemeProvider } from '@material-ui/core';
import Search from './Search';
import Results from './Results';
import { RESULTS } from '../constants/mockResults';

const darkTheme = createMuiTheme({
  palette: {
    type: 'dark',
  },
});

const App = () => {
  const [results, setResults] = useState([]);

  const fetchSearchResults = async query => {
    // const response = await fetch(`/search?q=${query}`);
    // const queryResults = await response.json();
    const queryResults = RESULTS; // TODO: testing
    setResults(queryResults);
  };

  return (
    <ThemeProvider theme={darkTheme}>
      <Search handleSearch={fetchSearchResults} isShowingResults={results.length > 0} />
      <Results results={results} />
    </ThemeProvider>
  );
};

export default App;
