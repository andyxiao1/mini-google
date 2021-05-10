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
  const [isLoading, setIsLoading] = useState(false);

  const fetchSearchResults = async query => {
    // REAL CODE
    // setIsLoading(true);
    // const response = await fetch(`/search?q=${query}`);
    // const queryResults = await response.json();
    // setResults(queryResults);
    // setIsLoading(false);

    // TESTING CODE
    setIsLoading(true);
    setTimeout(() => {
      const queryResults = RESULTS;
      setResults(queryResults);
      setIsLoading(false);
    }, 2000);
  };

  return (
    <ThemeProvider theme={darkTheme}>
      <Search handleSearch={fetchSearchResults} isShowingResults={results.length > 0 || isLoading} />
      <Results results={results} isLoading={isLoading} />
    </ThemeProvider>
  );
};

export default App;
