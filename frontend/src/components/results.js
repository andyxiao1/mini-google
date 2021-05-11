import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import Loader from './Loader';
import List from '@material-ui/core/List';
import Container from '@material-ui/core/Container';
import ListItem from '@material-ui/core/ListItem';
import Divider from '@material-ui/core/Divider';
import ListItemText from '@material-ui/core/ListItemText';
import Typography from '@material-ui/core/Typography';

const useStyles = makeStyles(theme => ({
  container: {
    marginLeft: 180,
    flex: 1,
    display: 'flex',
  },
  list: {
    marginTop: theme.spacing(1),
    alignItems: 'center',
    justifyContent: 'center',
    border: 20,
    borderRadius: 3,
    width: '100%',
  },
  loader: {
    marginTop: -theme.spacing(16),
    zoom: 0.5,
    position: 'absolute',
    left: '50%',
    top: '50%',
    transform: 'translate(-50%)',
  },
  title: { color: '#42a9db' },
  url: { color: '#0c942e' },
}));

const Results = ({ results, isLoading }) => {
  const classes = useStyles();

  if (!isLoading && (!results || results.length === 0)) {
    return null;
  }

  const resultslist = results.map((result, i) => {
    return (
      <div key={i}>
        <ListItem alignItems="flex-start" button component="a" href={result.url}>
          <ListItemText
            primary={
              <>
                <Typography variant="h6" className={classes.title}>
                  {result.title.slice(0, 61)}
                </Typography>
                <Typography variant="subtitle1" className={classes.url}>
                  {result.url}
                </Typography>
              </>
            }
            secondary={result.docExcerpt}
          />
        </ListItem>
        <Divider variant="fullWidth" component="li" />
      </div>
    );
  });

  return (
    <Container className={classes.container} maxWidth={'md'}>
      {isLoading ? (
        <div className={classes.loader}>
          <Loader />
        </div>
      ) : (
        <List className={classes.list}>{resultslist}</List>
      )}
    </Container>
  );
};

export default Results;
