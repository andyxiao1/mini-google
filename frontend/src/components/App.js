import React, { useState } from 'react';
import { createMuiTheme, ThemeProvider } from '@material-ui/core';
import Search from './Search';
import Results from './Results';
import { RESULTS } from '../constants/mockResults';

const SERVER_URL = 'http://ec2-100-26-9-51.compute-1.amazonaws.com:45555/';

const darkTheme = createMuiTheme({
  palette: {
    type: 'dark',
  },
});

const App = () => {
  const [results, setResults] = useState([]);
  const [isLoading, setIsLoading] = useState(false);

  const fetchSearchResults = async q => {
    // REAL CODE
    setIsLoading(true);
    const response = await fetch(`${SERVER_URL}/search?` + new URLSearchParams({ q }));
    let queryResults = await response.json();
    queryResults = queryResults.map(item => JSON.parse(item));
    // console.log(queryResults);
    setResults(queryResults);
    setIsLoading(false);

    // TESTING CODE
    // setIsLoading(true);
    // setTimeout(() => {
    //   const queryResults = RESULTS.map(item => JSON.parse(item));
    //   setResults(queryResults);
    //   setIsLoading(false);
    // }, 2000);
  };

  return (
    <ThemeProvider theme={darkTheme}>
      <Search handleSearch={fetchSearchResults} isShowingResults={results.length > 0 || isLoading} />
      <Results results={results} isLoading={isLoading} />
    </ThemeProvider>
  );
};

export default App;
