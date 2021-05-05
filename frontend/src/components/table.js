import React from 'react'
import {RESULTS} from  './results'
import { Media } from 'reactstrap';
import { makeStyles } from '@material-ui/core/styles';
import List from '@material-ui/core/List';
import ListItem from '@material-ui/core/ListItem';
import Divider from '@material-ui/core/Divider';
import ListItemText from '@material-ui/core/ListItemText';
import ListItemAvatar from '@material-ui/core/ListItemAvatar';
import Avatar from '@material-ui/core/Avatar';
import Typography from '@material-ui/core/Typography';



export default function Table(props) {
	// constructor(props){
	// 	super(props);
	//
  //   props = {
	// 		     results: RESULTS
	// 	};
	// }

	const useStyles = makeStyles((theme) => ({
	  root: {
			alignItems: 'center',
			justifyContent: 'center',
			border: 20,
			borderRadius: 3,
	    width: '100%',
	    maxWidth: '200ch',
	    backgroundColor: theme.palette.background.paper,
			margin: theme.spacing(5)

	  },
	  inline: {
	    display: 'inline',
	  },
	}));

const classes = useStyles();




		const resultslist = props.results.map((result) => {
			return(
				<div>
				<ListItem alignItems="flex-start" button component="a" href={result.url}>
			        <ListItemText
			          primary= {result.title}
			          secondary={
			            <React.Fragment>
			              <Typography
			                component="span"
			                variant="body2"
			                className={classes.inline}
			                color="textPrimary"
			              >
			              </Typography>
			              {result.text.substring(0,100)}
			            </React.Fragment>
			          }
			        />
			      </ListItem>
			      <Divider variant="fullWidth" component="li" />
				</div>
			);
		});

		// return(
		// 	<div className="container">
		// 		<div className="row">
		// 			<Media list>
		// 				{menu}
		// 			</Media>
		// 		</div>
		// 	</div>
		// );


		return (
		    <List className={classes.root}>
					{resultslist}
		    </List>
		  );
	}
