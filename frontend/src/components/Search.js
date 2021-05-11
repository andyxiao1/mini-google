import React, { useState } from 'react';
import Container from '@material-ui/core/Container';
import CssBaseline from '@material-ui/core/CssBaseline';
import TextField from '@material-ui/core/TextField';
import SearchRounded from '@material-ui/icons/SearchRounded';
import InputAdornment from '@material-ui/core/InputAdornment';
import { makeStyles } from '@material-ui/core/styles';

const useStyles = makeStyles(theme => ({
  container: {
    minHeight: '100%',
    flex: isShowingResults => (isShowingResults ? 0 : 1),
    display: 'flex',
    marginLeft: isShowingResults => (isShowingResults ? 180 : 'auto'),
    position: 'sticky',
    top: 0,
    backgroundColor: '#303033',
    zIndex: 10,
  },
  paper: {
    marginTop: isShowingResults => (isShowingResults ? 0 : -theme.spacing(32)),
    display: 'flex',
    flexDirection: 'column',
    alignItems: 'center',
    justifyContent: 'center',
    minHeight: '100%',
    flex: 1,
  },
  avatar: {
    margin: theme.spacing(1),
    backgroundColor: theme.palette.secondary.contrastText,
  },
  searchBar: {
    width: '100%',
    marginTop: theme.spacing(1),
  },
}));

const Search = ({ handleSearch, isShowingResults }) => {
  const classes = useStyles(isShowingResults);
  const [query, setQuery] = useState('');

  const handleSubmit = e => {
    e.preventDefault();
    handleSearch(query);
  };

  return (
    <Container className={classes.container} maxWidth={'md'}>
      <CssBaseline />
      <div className={classes.paper}>
        <form onSubmit={handleSubmit} className={classes.searchBar}>
          <TextField
            variant="outlined"
            margin="normal"
            fullWidth
            id="email"
            label="What do you want to search?"
            name="search"
            value={query}
            onChange={e => setQuery(e.target.value)}
            autoComplete="search"
            autoFocus
            InputProps={{
              startAdornment: (
                <InputAdornment position="start">
                  <SearchRounded />
                </InputAdornment>
              ),
            }}
          />
        </form>
      </div>
    </Container>
  );
};

export default Search;
