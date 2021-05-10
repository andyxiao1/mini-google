import React from 'react';
import { makeStyles } from '@material-ui/core/styles';
import List from '@material-ui/core/List';
import Container from '@material-ui/core/Container';
import ListItem from '@material-ui/core/ListItem';
import Divider from '@material-ui/core/Divider';
import ListItemText from '@material-ui/core/ListItemText';
import Typography from '@material-ui/core/Typography';

const useStyles = makeStyles(theme => ({
  container: {
    marginLeft: 180,
  },
  list: {
    marginTop: theme.spacing(1),
    alignItems: 'center',
    justifyContent: 'center',
    border: 20,
    borderRadius: 3,
    width: '100%',
    // maxWidth: '200ch',
  },
  inline: {
    display: 'inline',
  },
}));

const Results = ({ results }) => {
  const classes = useStyles();

  if (!results || results.length === 0) {
    return null;
  }

  const resultslist = results.map(result => {
    return (
      <div>
        <ListItem alignItems="flex-start" button component="a" href={result.url}>
          <ListItemText
            primary={result.title}
            secondary={
              <React.Fragment>
                <Typography
                  component="span"
                  variant="body2"
                  className={classes.inline}
                  color="textPrimary"
                ></Typography>
                {result.text}
              </React.Fragment>
            }
          />
        </ListItem>
        <Divider variant="fullWidth" component="li" />
      </div>
    );
  });

  return (
    <Container className={classes.container} maxWidth={'md'}>
      <List className={classes.list}>{resultslist}</List>
    </Container>
  );
};

export default Results;
